package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	ufssql "github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

func dbPath(cfgDir string) string {
	return filepath.Join(cfgDir, "db")
}

func carPath(cfgDir string) string {
	return filepath.Join(cfgDir, "carstore")
}

func configureRepo(ctx context.Context, cfgDir string) (*sql.DB, error) {
	if cfgDir == "" {
		return nil, fmt.Errorf("%s is a required flag", FlagRepo.Name)
	}

	if err := os.MkdirAll(cfgDir, 0744); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(carPath(cfgDir), 0744); err != nil {
		return nil, err
	}

	db, err := ufssql.SqlDB(dbPath(cfgDir))
	if err != nil {
		return nil, err
	}
	err = ufssql.CreateTables(ctx, db)
	return db, err
}

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Init stargate config",
	Before: before,
	Flags:  []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}
		_, err = configureRepo(cctx.Context, repoDir)
		if err == nil {
			fmt.Println("Stargate activated!")
		}
		return err
	},
}
