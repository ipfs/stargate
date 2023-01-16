package traversal_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	quickbuilder "github.com/ipfs/go-unixfsnode/data/builder/quick"
	"github.com/ipfs/stargate/pkg/unixfsstore/traversal"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/require"
)

func TestTraversal(t *testing.T) {

	ctx := context.Background()
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: make(map[string][]byte)}
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
	basicDirPathRecords := []pathRecord{}
	for key, file := range basicDir {
		basicDirPathRecords = append(basicDirPathRecords, pathRecord{dirLink, key, []cid.Cid{file.Link().(cidlink.Link).Cid}})
	}
	sort.Slice(basicDirPathRecords, func(i, j int) bool {
		return basicDirPathRecords[i].path < basicDirPathRecords[j].path
	})

	testCases := []struct {
		name             string
		root             cid.Cid
		recursive        bool
		expectedRoots    []rootRecord
		verifyPaths      func(*testing.T, []pathRecord)
		verifyFileRanges func(*testing.T, []fileRangeRecord)
		expectedErr      error
	}{
		{
			name:          "basic directory",
			root:          dirLink,
			expectedRoots: []rootRecord{{dirLink, data.Data_Directory}},
			verifyPaths: func(t *testing.T, receivedRecords []pathRecord) {
				require.Equal(t, basicDirPathRecords, receivedRecords)
			},
		},
		{
			name:             "file",
			root:             fileLink,
			expectedRoots:    []rootRecord{{fileLink, data.Data_File}},
			verifyFileRanges: verifyFileRanges(fileLink, 1<<22, 4096),
		},
		{
			name:      "recursive",
			recursive: true,
			root:      recursiveFolderLink.(cidlink.Link).Cid,
			expectedRoots: []rootRecord{
				{recursiveFolderLink.(cidlink.Link).Cid, data.Data_Directory},
				{subFolderLink.(cidlink.Link).Cid, data.Data_Directory},
				{fileLink, data.Data_File},
			},
			verifyPaths: func(t *testing.T, receivedRecords []pathRecord) {
				require.Equal(t, []pathRecord{
					{recursiveFolderLink.(cidlink.Link).Cid, "subfolder", []cid.Cid{subFolderLink.(cidlink.Link).Cid}},
					{subFolderLink.(cidlink.Link).Cid, "file.txt", []cid.Cid{fileLink}},
				}, receivedRecords)
			},
			verifyFileRanges: verifyFileRanges(fileLink, 1<<22, 4096),
		},
		{
			name:          "hamt",
			root:          hamtLink,
			expectedRoots: []rootRecord{{hamtLink, data.Data_HAMTShard}},
			verifyPaths:   verifyHAMTPaths(hamtLink, hamtDir),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			rv := &recordingVisitor{}
			var visitor traversal.UnixFSVisitor = rv
			if testCase.recursive {
				visitor = traversal.RecursiveVisitor(visitor, &ls)
			}
			err := traversal.IterateUnixFSNode(ctx, testCase.root, &ls, visitor)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedRoots, rv.rootRecords)
				if testCase.verifyPaths != nil {
					testCase.verifyPaths(t, rv.pathRecords)
				}
				if testCase.verifyFileRanges != nil {
					testCase.verifyFileRanges(t, rv.fileRangeRecords)
				}
			} else {
				require.EqualError(t, err, testCase.expectedErr.Error())
			}
		})
	}
}

type rootRecord struct {
	root cid.Cid
	kind int64
}

type pathRecord struct {
	root cid.Cid
	path string
	cids []cid.Cid
}

type fileRangeRecord struct {
	root    cid.Cid
	cid     cid.Cid
	depth   int
	byteMin uint64
	byteMax uint64
	leaf    bool
}

type recordingVisitor struct {
	rootRecords      []rootRecord
	pathRecords      []pathRecord
	fileRangeRecords []fileRangeRecord
}

func (rv *recordingVisitor) OnPath(ctx context.Context, root cid.Cid, path string, cids []cid.Cid) error {
	rv.pathRecords = append(rv.pathRecords, pathRecord{root, path, cids})
	return nil
}

func (rv *recordingVisitor) OnFileRange(ctx context.Context, root cid.Cid, cid cid.Cid, depth int, byteMin uint64, byteMax uint64, leaf bool) error {
	rv.fileRangeRecords = append(rv.fileRangeRecords, fileRangeRecord{root, cid, depth, byteMin, byteMax, leaf})
	return nil
}

func (rv *recordingVisitor) OnRoot(ctx context.Context, root cid.Cid, kind int64) error {
	rv.rootRecords = append(rv.rootRecords, rootRecord{root, kind})
	return nil
}

func verifyFileRanges(root cid.Cid, size uint64, chunkSize uint64) func(*testing.T, []fileRangeRecord) {
	expectedRecords := []fileRangeRecord{}
	totalDepth := 0
	var layerSize uint64 = uint64(builder.DefaultLinksPerBlock) * chunkSize
	for layerSize < size {
		totalDepth++
		layerSize *= uint64(builder.DefaultLinksPerBlock)
	}
	for pos := uint64(0); pos < size; pos += chunkSize {
		end := pos + chunkSize
		if end > size {
			end = size
		}
		expectedRecords = append(expectedRecords, fileRangeRecord{root, cid.Undef, totalDepth, pos, end, true})
	}
	layerSize = uint64(builder.DefaultLinksPerBlock) * chunkSize
	for totalDepth > 0 {
		for pos := uint64(0); pos < size; pos += layerSize {
			end := pos + layerSize
			if end > size {
				end = size
			}
			expectedRecords = append(expectedRecords, fileRangeRecord{root, cid.Undef, totalDepth - 1, pos, end, false})
		}
		totalDepth--
		layerSize *= uint64(builder.DefaultLinksPerBlock)
	}
	return func(t *testing.T, receivedRecords []fileRangeRecord) {
		for _, rr := range receivedRecords {
			found := false
			for _, er := range expectedRecords {
				if er.byteMax == rr.byteMax && er.byteMin == rr.byteMin {
					require.Equal(t, er.root, rr.root)
					require.Equal(t, er.leaf, rr.leaf)
					require.Equal(t, er.depth, rr.depth)
					found = true
					break
				}
			}
			require.True(t, found)
		}
		require.Len(t, receivedRecords, len(expectedRecords))
	}
}

func verifyHAMTPaths(root cid.Cid, hamtPaths map[string]quickbuilder.Node) func(*testing.T, []pathRecord) {
	return func(t *testing.T, receivedRecords []pathRecord) {
		for _, rr := range receivedRecords {
			found := false
			for path, node := range hamtPaths {
				expectedCid := node.Link().(cidlink.Link).Cid
				if path == rr.path {
					if expectedCid.Equals(rr.cids[len(rr.cids)-1]) {
						require.Equal(t, root, rr.root)
						found = true
						break
					}
				}
			}
			require.True(t, found)
		}
		require.Len(t, receivedRecords, len(hamtPaths))
	}
}
