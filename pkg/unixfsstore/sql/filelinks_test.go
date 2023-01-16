package sql_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/internal/testutil"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/stretchr/testify/require"
)

func TestFileLinksDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(sql.CreateTables(ctx, sqldb))

	rootCid := testutil.GenerateCid()
	intermediates := testutil.GenerateCids(10)
	for i, c := range intermediates {
		err := sql.InsertFileLink(ctx, sqldb, &sql.FileLink{
			RootCID:  rootCid,
			Metadata: []byte("orange"),
			CID:      c,
			Depth:    0,
			Leaf:     false,
			ByteMin:  uint64(i) * (1 << 22) * 10,
			ByteMax:  uint64(i+1) * (1 << 22) * 10,
		})
		req.NoError(err)
	}

	leaves := testutil.GenerateCids(100)
	for i, c := range leaves {
		err := sql.InsertFileLink(ctx, sqldb, &sql.FileLink{
			RootCID:  rootCid,
			Metadata: []byte("orange"),
			CID:      c,
			Depth:    1,
			Leaf:     true,
			ByteMin:  uint64(i) * (1 << 22),
			ByteMax:  uint64(i+1) * (1 << 22),
		})
		req.NoError(err)
	}

	otherRootCid := testutil.GenerateCid()
	otherIntermediates := testutil.GenerateCids(10)
	for i, c := range otherIntermediates {
		err := sql.InsertFileLink(ctx, sqldb, &sql.FileLink{
			RootCID:  otherRootCid,
			Metadata: nil,
			CID:      c,
			Depth:    0,
			Leaf:     false,
			ByteMin:  uint64(i) * (1 << 22) * 10,
			ByteMax:  uint64(i+1) * (1 << 22) * 10,
		})
		req.NoError(err)
	}

	otherLeaves := testutil.GenerateCids(100)
	for i, c := range otherLeaves {
		err := sql.InsertFileLink(ctx, sqldb, &sql.FileLink{
			RootCID:  otherRootCid,
			Metadata: nil,
			CID:      c,
			Depth:    1,
			Leaf:     true,
			ByteMin:  uint64(i) * (1 << 22),
			ByteMax:  uint64(i+1) * (1 << 22),
		})
		req.NoError(err)
	}

	// test full
	rootAll, err := sql.FileAll(ctx, sqldb, rootCid, []byte("orange"))
	req.NoError(err)
	req.Equal([][]cid.Cid{
		intermediates,
		leaves,
	},
		rootAll)

	otherRootAll, err := sql.FileAll(ctx, sqldb, otherRootCid, nil)
	req.NoError(err)
	req.Equal([][]cid.Cid{
		otherIntermediates,
		otherLeaves,
	},
		otherRootAll)

	// test byte range
	rootRange, err := sql.FileByteRange(ctx, sqldb, rootCid, []byte("orange"), 10*(1<<22), 21*(1<<22))
	req.NoError(err)
	req.Equal([][]cid.Cid{
		{intermediates[1], intermediates[2]},
		leaves[10:21],
	},
		rootRange)

}
