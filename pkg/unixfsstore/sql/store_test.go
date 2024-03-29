package sql_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: make(map[string][]byte)}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)

	delimited := io.LimitReader(rand.Reader, 1<<22)
	n, sz, err := builder.BuildUnixFSFile(delimited, "size-4096", &ls)
	req.NoError(err)
	fileLink := n.(cidlink.Link).Cid
	dirEntry, err := builder.BuildUnixFSDirectoryEntry("file.txt", int64(sz), n)
	req.NoError(err)
	subFolderLink, sz, err := builder.BuildUnixFSDirectory([]dagpb.PBLink{dirEntry}, &ls)
	req.NoError(err)
	dirEntry, err = builder.BuildUnixFSDirectoryEntry("subfolder", int64(sz), subFolderLink)
	req.NoError(err)
	recursiveFolderLink, _, err := builder.BuildUnixFSDirectory([]dagpb.PBLink{dirEntry}, &ls)
	req.NoError(err)

	sqldb := CreateTestTmpDB(t)
	req.NoError(sql.CreateTables(ctx, sqldb))

	db := sql.NewSQLUnixFSStore(sqldb)
	err = db.AddRootRecursive(ctx, recursiveFolderLink.(cidlink.Link).Cid, []byte("apples"), &ls)
	req.NoError(err)

	rootCids, err := db.RootCID(ctx, recursiveFolderLink.(cidlink.Link).Cid)
	req.NoError(err)
	req.ElementsMatch([]unixfsstore.RootCID{
		{
			CID:      recursiveFolderLink.(cidlink.Link).Cid,
			Kind:     data.Data_Directory,
			Metadata: []byte("apples"),
		},
	}, rootCids)

	rootCid, err := db.RootCIDWithMetadata(ctx, recursiveFolderLink.(cidlink.Link).Cid, []byte("apples"))
	req.NoError(err)
	req.Equal(&unixfsstore.RootCID{
		CID:      recursiveFolderLink.(cidlink.Link).Cid,
		Kind:     data.Data_Directory,
		Metadata: []byte("apples"),
	}, rootCid)
	rootLinks, err := db.DirLs(ctx, recursiveFolderLink.(cidlink.Link).Cid, []byte("apples"))
	req.NoError(err)
	req.Equal([][]unixfsstore.TraversedCID{
		{
			{CID: subFolderLink.(cidlink.Link).Cid, IsLeaf: true},
		},
	}, rootLinks)

	rootPath, err := db.DirPath(ctx, recursiveFolderLink.(cidlink.Link).Cid, []byte("apples"), "subfolder")
	req.NoError(err)
	req.Equal([]cid.Cid{subFolderLink.(cidlink.Link).Cid}, rootPath)

	subFolderCids, err := db.RootCID(ctx, subFolderLink.(cidlink.Link).Cid)
	req.NoError(err)
	req.ElementsMatch([]unixfsstore.RootCID{
		{
			CID:      subFolderLink.(cidlink.Link).Cid,
			Kind:     data.Data_Directory,
			Metadata: []byte("apples"),
		},
	}, subFolderCids)
	req.NoError(err)

	subFolderLinks, err := db.DirLs(ctx, subFolderLink.(cidlink.Link).Cid, []byte("apples"))
	req.NoError(err)
	req.Equal([][]unixfsstore.TraversedCID{
		{{CID: fileLink, IsLeaf: true}},
	}, subFolderLinks)

	subFolderPath, err := db.DirPath(ctx, subFolderLink.(cidlink.Link).Cid, []byte("apples"), "file.txt")
	req.NoError(err)
	req.Equal([]cid.Cid{fileLink}, subFolderPath)

	fileLayers, err := db.FileAll(ctx, fileLink, []byte("apples"))
	req.NoError(err)
	req.NotEmpty(fileLayers)
}
