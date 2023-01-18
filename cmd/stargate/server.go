package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/internal/stores"
	"github.com/ipfs/stargate/internal/storeutil"
	stargate "github.com/ipfs/stargate/pkg"
	"github.com/ipfs/stargate/pkg/handler"
	"github.com/ipfs/stargate/pkg/unixfsresolver"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/ipld/go-ipld-prime"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var serverCmd = &cli.Command{
	Name:   "server",
	Usage:  "Start a stargate http server",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6070",
		},
		&cli.UintFlag{
			Name:  "port",
			Usage: "the port the web server listens on",
			Value: 7777,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("pprof") {
			go func() {
				err := http.ListenAndServe("localhost:6070", nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}
		sqldb, err := configureRepo(cctx.Context, repoDir)
		if err != nil {
			return fmt.Errorf("initializing repo: %w", err)
		}
		db := sql.NewSQLUnixFSStore(sqldb)
		unixFSAppResolver := unixfsresolver.NewUnixFSAppResolver(db, &resolver{})
		server := NewHttpServer(
			cctx.Int("port"),
			map[string]stargate.AppResolver{
				"ipfs": unixFSAppResolver,
			},
		)

		// Start the server
		log.Infof("Opening a stargate on port %d",
			cctx.Int("port"))
		server.Start(cctx.Context)

		// Monitor for shutdown.
		<-cctx.Context.Done()

		log.Info("Shutting down stargate...")

		err = server.Stop()
		if err != nil {
			return err
		}
		log.Info("Graceful shutdown successful")

		// Sync all loggers.
		_ = log.Sync() //nolint:errcheck

		return nil
	},
}

type resolver struct{}

func (r resolver) ResolveLinkSystem(ctx context.Context, root cid.Cid, metadata []byte) (*ipld.LinkSystem, error) {
	carFile := string(metadata)
	bs, err := stores.ReadOnlyFilestore(carFile)
	if err != nil {
		return nil, err
	}
	ls := storeutil.LinkSystemForBlockstore(bs)
	return &ls, nil
}

type HttpServer struct {
	port   int
	apps   map[string]stargate.AppResolver
	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

func NewHttpServer(port int, apps map[string]stargate.AppResolver) *HttpServer {
	return &HttpServer{port: port, apps: apps}
}

func (s *HttpServer) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	listenAddr := fmt.Sprintf(":%d", s.port)
	server := http.NewServeMux()
	for key, resolver := range s.apps {
		h := handler.NewHandler(key, resolver)
		server.Handle("/"+key+"/", h)
	}
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: server,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return s.ctx
		},
	}

	go func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %w", err)
		}
	}()
}

func (s *HttpServer) Stop() error {
	s.cancel()
	return s.server.Close()
}
