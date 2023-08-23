package sql_test

import (
	"context"
	"testing"

	"github.com/ipfs/stargate/internal/testutil"
	"github.com/ipfs/stargate/pkg/unixfsstore"
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
	traversedIntermediates := make([]unixfsstore.TraversedCID, 0, 10)
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
		traversedIntermediates = append(traversedIntermediates, unixfsstore.TraversedCID{CID: c, IsLeaf: false})
	}

	leaves := testutil.GenerateCids(100)
	traversedLeaves := make([]unixfsstore.TraversedCID, 0, 100)
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
		traversedLeaves = append(traversedLeaves, unixfsstore.TraversedCID{CID: c, IsLeaf: true})
	}

	otherRootCid := testutil.GenerateCid()
	otherIntermediates := testutil.GenerateCids(10)
	otherTraversedIntermediates := make([]unixfsstore.TraversedCID, 0, 10)
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
		otherTraversedIntermediates = append(otherTraversedIntermediates, unixfsstore.TraversedCID{CID: c, IsLeaf: false})
	}

	otherLeaves := testutil.GenerateCids(100)
	otherTraversedLeaves := make([]unixfsstore.TraversedCID, 0, 100)
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
		otherTraversedLeaves = append(otherTraversedLeaves, unixfsstore.TraversedCID{CID: c, IsLeaf: true})
	}

	// test full
	rootAll, err := sql.FileAll(ctx, sqldb, rootCid, []byte("orange"))
	req.NoError(err)
	req.Equal([][]unixfsstore.TraversedCID{
		traversedIntermediates,
		traversedLeaves,
	},
		rootAll)

	otherRootAll, err := sql.FileAll(ctx, sqldb, otherRootCid, nil)
	req.NoError(err)
	req.Equal([][]unixfsstore.TraversedCID{
		otherTraversedIntermediates,
		otherTraversedLeaves,
	},
		otherRootAll)

	// test byte range
	rootRange, err := sql.FileByteRange(ctx, sqldb, rootCid, []byte("orange"), 10*(1<<22), 21*(1<<22))
	req.NoError(err)

	req.Equal([][]unixfsstore.TraversedCID{
		{traversedIntermediates[1], traversedIntermediates[2]},
		traversedLeaves[10:21],
	},
		rootRange)

}
