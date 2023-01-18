package sql_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/stargate/internal/testutil"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/stretchr/testify/require"
)

func TestRootCIDSDb(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(sql.CreateTables(ctx, sqldb))

	rootCid := testutil.GenerateCid()
	err := sql.InsertRootCID(ctx, sqldb, unixfsstore.RootCID{
		CID:      rootCid,
		Kind:     data.Data_File,
		Metadata: []byte("apples"),
	})
	req.NoError(err)
	err = sql.InsertRootCID(ctx, sqldb, unixfsstore.RootCID{
		CID:      rootCid,
		Kind:     data.Data_File,
		Metadata: nil,
	})
	req.NoError(err)
	otherRootCid := testutil.GenerateCid()
	err = sql.InsertRootCID(ctx, sqldb, unixfsstore.RootCID{
		CID:      otherRootCid,
		Kind:     data.Data_HAMTShard,
		Metadata: []byte("oranges"),
	})
	req.NoError(err)

	missingRootCid := testutil.GenerateCid()

	rootCids, err := sql.RootCID(ctx, sqldb, rootCid)
	req.NoError(err)
	req.ElementsMatch([]unixfsstore.RootCID{
		{
			CID:      rootCid,
			Kind:     data.Data_File,
			Metadata: nil,
		},
		{
			CID:      rootCid,
			Kind:     data.Data_File,
			Metadata: []byte("apples"),
		},
	}, rootCids)

	returnedRootCid, err := sql.RootCIDWithMetadata(ctx, sqldb, rootCid, []byte("apples"))
	req.NoError(err)
	req.Equal(&unixfsstore.RootCID{
		CID:      rootCid,
		Kind:     data.Data_File,
		Metadata: []byte("apples"),
	}, returnedRootCid)

	returnedRootCid, err = sql.RootCIDWithMetadata(ctx, sqldb, rootCid, nil)
	req.NoError(err)
	req.Equal(&unixfsstore.RootCID{
		CID:      rootCid,
		Kind:     data.Data_File,
		Metadata: nil,
	}, returnedRootCid)
	rootCids, err = sql.RootCID(ctx, sqldb, otherRootCid)
	req.NoError(err)
	req.ElementsMatch([]unixfsstore.RootCID{
		{
			CID:      otherRootCid,
			Kind:     data.Data_HAMTShard,
			Metadata: []byte("oranges"),
		},
	}, rootCids)

	missingRootCids, err := sql.RootCID(ctx, sqldb, missingRootCid)
	req.Empty(missingRootCids)
	req.NoError(err)
}
