package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/stargate/internal/stores"
	"github.com/ipfs/stargate/internal/storeutil"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/ipfs/stargate/pkg/unixfsstore/traversal"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var importCmd = &cli.Command{
	Name:   "import",
	Usage:  "Import a file into the StarGate",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "car",
			Usage: "Imports a car file directly",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: import <filepath>")
		}

		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}
		sqldb, err := configureRepo(cctx.Context, repoDir)
		if err != nil {
			return fmt.Errorf("initializing repo: %w", err)
		}
		srcName, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding source file path: %w", err)
		}
		srcName, err = filepath.Abs(srcName)
		if err != nil {
			return fmt.Errorf("expanding source file path: %w", err)
		}

		root, carFileName, err := writeRawCarFile(repoDir, srcName)
		defer func() {
			if err != nil && carFileName != "" {
				_ = os.Remove(carFileName)
			}
		}()
		if err != nil {
			return err
		}
		newLocale := filepath.Join(carPath(repoDir), root.String()+".car")
		if fileExists(newLocale) {
			return errors.New("File or directory already imported")
		}
		err = os.Rename(carFileName, newLocale)
		if err != nil {
			return fmt.Errorf("Renaming file: %w", err)
		}
		carFileName = newLocale

		db := sql.NewSQLUnixFSStore(sqldb)
		err = indexImport(cctx.Context, carFileName, db)
		if err != nil {
			return fmt.Errorf("indexing the imported data: %w", err)
		}
		fmt.Printf("Sending CID %s through the Stargate!\n", root.String())
		return err
	},
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func writeRawCarFile(repoDir string, srcName string) (cid.Cid, string, error) {
	f, err := os.CreateTemp(carPath(repoDir), "stargate-tmp-")
	if err != nil {
		return cid.Undef, "", fmt.Errorf("creating CAR: %w", err)
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	bs, err := stores.ReadWriteFilestoreFile(f)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("cpening CAR Blockstore: %w", err)
	}
	lsys := storeutil.LinkSystemForBlockstore(bs)
	root, _, err := builder.BuildUnixFSRecursive(srcName, &lsys)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("importing data: %w", err)
	}
	err = bs.Close()
	if err != nil {
		return cid.Undef, "", fmt.Errorf("finalizing CAR file: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		return cid.Undef, "", fmt.Errorf("reading CAR stats: %w", err)
	}
	existingName := filepath.Join(carPath(repoDir), st.Name())

	if err = f.Close(); err != nil {
		return cid.Undef, "", fmt.Errorf("closing car file: %w", err)
	}
	return root.(cidlink.Link).Cid, existingName, nil
}

func indexImport(ctx context.Context, carFileName string, db *sql.SQLUnixFSStore) error {

	bs, err := stores.ReadOnlyFilestore(carFileName)
	if err != nil {
		return fmt.Errorf("Reopening file store")
	}
	allKeys, err := bs.AllKeysChan(ctx)
	if err != nil {
		return fmt.Errorf("Fetching all block keys")
	}
	lsys := storeutil.LinkSystemForBlockstore(bs)

	roots, err := traversal.DiscoverRoots(ctx, allKeys, &lsys)
	if err != nil {
		return fmt.Errorf("Discovering roots: %w", err)
	}

	for _, root := range roots {
		err := db.AddRoot(ctx, root, []byte(carFileName), &lsys)
		if err != nil {
			return fmt.Errorf("Adding root to index: %w", err)
		}
	}
	return bs.Close()
}
