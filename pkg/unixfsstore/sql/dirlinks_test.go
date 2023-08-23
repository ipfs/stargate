package sql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/internal/testutil"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	ufssql "github.com/ipfs/stargate/pkg/unixfsstore/sql"
	"github.com/stretchr/testify/require"
)

func TestDirLinksDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(ufssql.CreateTables(ctx, sqldb))

	rootCid := testutil.GenerateCid()
	path1Cids := testutil.GenerateCids(3)
	traversedPath1Cids := make([]unixfsstore.TraversedCID, 0, 3)
	for i, c := range path1Cids {
		err := ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
			RootCID:  rootCid,
			Metadata: []byte("orange"),
			CID:      c,
			Depth:    uint64(i),
			Leaf:     (i == len(path1Cids)-1),
			SubPath:  "path1",
		})
		req.NoError(err)
		traversedPath1Cids = append(traversedPath1Cids, unixfsstore.TraversedCID{c, i == len(path1Cids)-1})
	}

	path2Cids := testutil.GenerateCids(3)
	traversedPath2Cids := make([]unixfsstore.TraversedCID, 0, 3)
	for i, c := range path2Cids {
		err := ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
			RootCID:  rootCid,
			Metadata: []byte("orange"),
			CID:      c,
			Depth:    uint64(i),
			Leaf:     (i == len(path2Cids)-1),
			SubPath:  "path2",
		})
		req.NoError(err)
		traversedPath2Cids = append(traversedPath2Cids, unixfsstore.TraversedCID{c, i == len(path2Cids)-1})
	}

	// insert a 3rd path, with some non-unique CIDs
	// should not get picked up cause of dup with path 1
	err := ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
		RootCID:  rootCid,
		Metadata: []byte("orange"),
		CID:      path1Cids[0],
		Depth:    0,
		Leaf:     false,
		SubPath:  "path3",
	})
	req.NoError(err)

	// will get picked up cause dup of path2 is not at the same level
	err = ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
		RootCID:  rootCid,
		Metadata: []byte("orange"),
		CID:      path2Cids[0],
		Depth:    1,
		Leaf:     false,
		SubPath:  "path3",
	})
	req.NoError(err)

	path3Leaf := testutil.GenerateCid()
	// will get picked up cause unique
	err = ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
		RootCID:  rootCid,
		Metadata: []byte("orange"),
		CID:      path3Leaf,
		Depth:    2,
		Leaf:     true,
		SubPath:  "path3",
	})
	req.NoError(err)

	// insert additional root path
	otherRootCid := testutil.GenerateCid()
	path1CidsOtherRoot := testutil.GenerateCids(3)
	for i, c := range path1CidsOtherRoot {
		err := ufssql.InsertDirLink(ctx, sqldb, &ufssql.DirLink{
			RootCID: otherRootCid,
			CID:     c,
			Depth:   uint64(i),
			Leaf:    (i == len(path1CidsOtherRoot)-1),
			SubPath: "path1",
		})
		req.NoError(err)
	}

	// test paths
	path1CidsQuery, err := ufssql.DirPath(ctx, sqldb, rootCid, []byte("orange"), "path1")
	req.NoError(err)
	req.Equal(path1Cids, path1CidsQuery)

	path2CidsQuery, err := ufssql.DirPath(ctx, sqldb, rootCid, []byte("orange"), "path2")
	req.NoError(err)
	req.Equal(path2Cids, path2CidsQuery)

	path3CidsQuery, err := ufssql.DirPath(ctx, sqldb, rootCid, []byte("orange"), "path3")
	req.NoError(err)
	req.Equal([]cid.Cid{path1Cids[0], path2Cids[0], path3Leaf}, path3CidsQuery)

	path1CidsOtherRootQuery, err := ufssql.DirPath(ctx, sqldb, otherRootCid, nil, "path1")
	req.NoError(err)
	req.Equal(path1CidsOtherRoot, path1CidsOtherRootQuery)

	// test ls
	rootCidsAll, err := ufssql.DirLs(ctx, sqldb, rootCid, []byte("orange"))
	req.NoError(err)
	req.Equal([][]unixfsstore.TraversedCID{
		{traversedPath1Cids[0], traversedPath2Cids[0]},
		{traversedPath1Cids[1], traversedPath2Cids[1], traversedPath2Cids[0]},
		{traversedPath1Cids[2], traversedPath2Cids[2], {path3Leaf, true}},
	},
		rootCidsAll)
}

func CreateTestTmpDB(t *testing.T) *sql.DB {
	f, err := os.CreateTemp(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	d, err := ufssql.SqlDB(f.Name())
	require.NoError(t, err)
	return d
}
