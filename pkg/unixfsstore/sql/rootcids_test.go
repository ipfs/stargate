package sql_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/stargate/internal/testutil"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/stretchr/testify/require"
)

func TestRootCIDSDb(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(sql.CreateTables(ctx, sqldb))

	rootCid := testutil.GenerateCid()
	err := sql.InsertRootCID(ctx, sqldb, rootCid, data.Data_File, []byte("apples"))
	req.NoError(err)
	err = sql.InsertRootCID(ctx, sqldb, rootCid, data.Data_File, nil)
	req.NoError(err)
	otherRootCid := testutil.GenerateCid()
	err = sql.InsertRootCID(ctx, sqldb, otherRootCid, data.Data_HAMTShard, []byte("oranges"))
	req.NoError(err)

	missingRootCid := testutil.GenerateCid()

	has, kind, err := sql.RootCID(ctx, sqldb, rootCid, []byte("apples"))
	req.NoError(err)
	req.True(has)
	req.Equal(data.Data_File, kind)

	has, kind, err = sql.RootCID(ctx, sqldb, rootCid, nil)
	req.NoError(err)
	req.True(has)
	req.Equal(data.Data_File, kind)

	has, kind, err = sql.RootCID(ctx, sqldb, otherRootCid, []byte("oranges"))
	req.NoError(err)
	req.True(has)
	req.Equal(data.Data_HAMTShard, kind)

	// must match metadata
	has, kind, err = sql.RootCID(ctx, sqldb, rootCid, []byte("oranges"))
	req.NoError(err)
	req.False(has)

	// must contain cid
	has, kind, err = sql.RootCID(ctx, sqldb, missingRootCid, []byte("apples"))
	req.NoError(err)
	req.False(has)

}
