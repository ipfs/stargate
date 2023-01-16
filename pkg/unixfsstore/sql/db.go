package sql

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("not found")

type Scannable interface {
	Scan(dest ...interface{}) error
}

func SqlDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err == nil {
		// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
		// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
		db.SetMaxOpenConns(1)
	}
	return db, err
}

//go:embed create_db.sql
var createDBSQL string

func CreateTables(ctx context.Context, mainDB *sql.DB) error {
	if _, err := mainDB.ExecContext(ctx, createDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in main DB: %w", err)
	}
	return nil
}

type Transactable interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
