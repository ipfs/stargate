package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	ufssql "github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

func configureRepo(cfgDir string) (*sql.DB, string, error) {
	if cfgDir == "" {
		return nil, "", fmt.Errorf("%s is a required flag", FlagRepo.Name)
	}

	if err := os.MkdirAll(cfgDir, 0744); err != nil {
		return nil, "", err
	}

	carDir := cfgDir + "/carstore"
	if err := os.MkdirAll(carDir, 0744); err != nil {
		return nil, "", err
	}
	carDir, err = filepath.Abs(carDir)
	if err != nil {
		return nil, "", err
	}

	sqlDir := cfgDir + "/db"
	if err := os.MkdirAll(sqlDir, 0744); err != nil {
		return nil, "", err
	}

	db, err := ufssql.SqlDB(sqlDir)
	if err != nil {
		return nil, "", err
	}
	return db, carDir, nil
}

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Init booster-bitswap config",
	Before: before,
	Flags:  []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}
		_, _, err = configureRepo(repoDir)
		fmt.Println("Stargate activated!")
		return err
	},
}
