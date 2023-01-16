package sql

import (
	"context"
	"database/sql"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/pkg/unixfsstore/traversal"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/mattn/go-sqlite3"
)

type unixFSVisitor struct {
	db       Transactable
	metadata []byte
}

func (ufsv *unixFSVisitor) OnPath(ctx context.Context, root cid.Cid, path string, cids []cid.Cid) error {
	for i, c := range cids {
		err := InsertDirLink(ctx, ufsv.db, &DirLink{
			RootCID:  root,
			Metadata: ufsv.metadata,
			CID:      c,
			Depth:    uint64(i),
			Leaf:     (i == len(cids)-1),
			SubPath:  path,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ufsv *unixFSVisitor) OnFileRange(ctx context.Context, root cid.Cid, cid cid.Cid, depth int, byteMin uint64, byteMax uint64, leaf bool) error {
	return InsertFileLink(ctx, ufsv.db, &FileLink{
		RootCID:  root,
		Metadata: ufsv.metadata,
		CID:      cid,
		Depth:    uint64(depth),
		Leaf:     leaf,
		ByteMin:  byteMin,
		ByteMax:  byteMax,
	})
}

func (ufsv *unixFSVisitor) OnRoot(ctx context.Context, root cid.Cid, kind int64) error {
	return InsertRootCID(ctx, ufsv.db, root, kind, ufsv.metadata)
}

type SQLUnixFSStore struct {
	db *sql.DB
}

func NewSQLUnixFSStore(db *sql.DB) *SQLUnixFSStore {
	return &SQLUnixFSStore{db: db}
}

func (s *SQLUnixFSStore) AddRoot(ctx context.Context, root cid.Cid, metadata []byte, linkSystem *ipld.LinkSystem) error {
	return withTransaction(ctx, s.db, func(tx *sql.Tx) error {
		visitor := &unixFSVisitor{tx, metadata}
		return traversal.IterateUnixFSNode(ctx, root, linkSystem, visitor)
	})
}

func (s *SQLUnixFSStore) AddRootRecursive(ctx context.Context, root cid.Cid, metadata []byte, linkSystem *ipld.LinkSystem) error {
	return withTransaction(ctx, s.db, func(tx *sql.Tx) error {
		var visitor traversal.UnixFSVisitor = &unixFSVisitor{tx, metadata}
		visitor = traversal.RecursiveVisitor(visitor, linkSystem)
		return traversal.IterateUnixFSNode(ctx, root, linkSystem, visitor)
	})
}

func (s *SQLUnixFSStore) DirLs(ctx context.Context, root cid.Cid, metadata []byte) ([][]cid.Cid, error) {
	return DirLs(ctx, s.db, root, metadata)
}

func (s *SQLUnixFSStore) DirPath(ctx context.Context, root cid.Cid, metadata []byte, path string) ([]cid.Cid, error) {
	return DirPath(ctx, s.db, root, metadata, path)
}

func (s *SQLUnixFSStore) FileAll(ctx context.Context, root cid.Cid, metadata []byte) ([][]cid.Cid, error) {
	return FileAll(ctx, s.db, root, metadata)
}

func (s *SQLUnixFSStore) FileByteRange(ctx context.Context, root cid.Cid, metadata []byte, byteMin uint64, byteMax uint64) ([][]cid.Cid, error) {
	return FileByteRange(ctx, s.db, root, metadata, byteMin, byteMax)
}

func (s *SQLUnixFSStore) RootCID(ctx context.Context, root cid.Cid, metadata []byte) (bool, int64, error) {
	return RootCID(ctx, s.db, root, metadata)
}

func withTransaction(ctx context.Context, db *sql.DB, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	tx, err = db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	err = f(tx)
	return
}
