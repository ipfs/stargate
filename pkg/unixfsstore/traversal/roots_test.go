package traversal_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	quickbuilder "github.com/ipfs/go-unixfsnode/data/builder/quick"
	"github.com/ipfs/stargate/pkg/unixfsstore/traversal"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/require"
)

func TestDiscoverRoots(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)
	ls := cidlink.DefaultLinkSystem()
	bag := make(map[string][]byte)
	store := memstore.Store{Bag: bag}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)

	delimited := io.LimitReader(rand.Reader, 1<<22)
	n, sz, err := builder.BuildUnixFSFile(delimited, "size-4096", &ls)
	require.NoError(t, err)
	fileLink := n.(cidlink.Link).Cid
	var hamtLink cid.Cid
	var dirLink cid.Cid
	hamtDir := map[string]quickbuilder.Node{}
	basicDir := map[string]quickbuilder.Node{}
	dirEntry, err := builder.BuildUnixFSDirectoryEntry("file.txt", int64(sz), n)
	require.NoError(t, err)
	subFolderLink, sz, err := builder.BuildUnixFSDirectory([]dagpb.PBLink{dirEntry}, &ls)
	require.NoError(t, err)
	dirEntry, err = builder.BuildUnixFSDirectoryEntry("subfolder", int64(sz), subFolderLink)
	require.NoError(t, err)
	recursiveFolderLink, _, err := builder.BuildUnixFSDirectory([]dagpb.PBLink{dirEntry}, &ls)
	require.NoError(t, err)
	err = quickbuilder.Store(&ls, func(b *quickbuilder.Builder) error {
		for i := 0; i < 10000; i++ {
			hamtDir[fmt.Sprintf("file%d.txt", i)] = b.NewBytesFile([]byte(fmt.Sprintf("data%d", i)))
		}
		hamtLink = b.NewMapDirectory(hamtDir).Link().(cidlink.Link).Cid
		for i := 0; i < 20; i++ {
			basicDir[fmt.Sprintf("filebasice%d.txt", i)] = b.NewBytesFile([]byte(fmt.Sprintf("databasic%d", i)))
		}
		dirLink = b.NewMapDirectory(basicDir).Link().(cidlink.Link).Cid
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedRoots := []cid.Cid{
		fileLink,
		subFolderLink.(cidlink.Link).Cid,
		recursiveFolderLink.(cidlink.Link).Cid,
		hamtLink,
		dirLink,
	}
	for _, entry := range hamtDir {
		expectedRoots = append(expectedRoots, entry.Link().(cidlink.Link).Cid)
	}
	for _, entry := range basicDir {
		expectedRoots = append(expectedRoots, entry.Link().(cidlink.Link).Cid)
	}
	allCids := make(chan cid.Cid)
	go func() {
		defer close(allCids)
		for key := range bag {
			_, c, err := cid.CidFromBytes([]byte(key))
			req.NoError(err)
			allCids <- c
		}
	}()
	roots, err := traversal.DiscoverRoots(ctx, allCids, &ls)
	req.NoError(err)
	req.ElementsMatch(expectedRoots, roots)
}
