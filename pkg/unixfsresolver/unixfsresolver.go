package unixfsresolver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	stargate "github.com/ipfs/stargate/pkg"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	"github.com/ipld/go-ipld-prime"
	"go.uber.org/multierr"
)

// UnixFSStore is an interface for fetching metadata about UnixFS queries
type UnixFSStore interface {
	DirLs(ctx context.Context, root cid.Cid, metadata []byte) ([][]unixfsstore.TraversedCID, error)
	DirPath(ctx context.Context, root cid.Cid, metadata []byte, path string) ([]cid.Cid, error)
	FileAll(ctx context.Context, root cid.Cid, metadata []byte) ([][]unixfsstore.TraversedCID, error)
	FileByteRange(ctx context.Context, root cid.Cid, metadata []byte, byteMin uint64, byteMax uint64) ([][]unixfsstore.TraversedCID, error)
	RootCID(ctx context.Context, root cid.Cid) ([]unixfsstore.RootCID, error)
	RootCIDWithMetadata(ctx context.Context, root cid.Cid, metadata []byte) (*unixfsstore.RootCID, error)
}

// LinkSystemResolves link systems from a root and associated metadata
type LinkSystemResolver interface {
	ResolveLinkSystem(ctx context.Context, root cid.Cid, metadata []byte) (*ipld.LinkSystem, error)
}

// NewUnixFSAppResolver returns a new UnixFS resolver using the given UnixFSStore and LinkSystemResolver
func NewUnixFSAppResolver(store UnixFSStore, linkSystemResolver LinkSystemResolver) *UnixFSAppResolver {
	return &UnixFSAppResolver{
		store:              store,
		linkSystemResolver: linkSystemResolver,
	}
}

// UnixFSAppResolver implements an AppResolver for the UnixFS domain
type UnixFSAppResolver struct {
	store              UnixFSStore
	linkSystemResolver LinkSystemResolver
}

// GetResolver attempts to resolve starting from the given root. It returns a linksystem to load blocks from
// and a resolver for the query
func (ufsar *UnixFSAppResolver) GetResolver(ctx context.Context, root cid.Cid) (*ipld.LinkSystem, stargate.PathResolver, error) {
	rootCids, err := ufsar.store.RootCID(ctx, root)
	if err != nil {
		return nil, nil, err
	}
	if len(rootCids) == 0 {
		return nil, nil, stargate.ErrNotFound{Cid: root}
	}
	// TODO: Find a better heuristic to decide on a linksystem than
	// "the first that works"
	var totalError error
	for _, returnedRootCid := range rootCids {
		lsys, err := ufsar.linkSystemResolver.ResolveLinkSystem(ctx, returnedRootCid.CID, returnedRootCid.Metadata)
		if err == nil {
			return lsys, &UnixFSResolver{ufsar.store, returnedRootCid}, nil
		}
		totalError = multierr.Append(totalError, err)
	}
	return nil, nil, totalError
}

// UnixFSResolver implements an PathResolver for the UnixFS domain
type UnixFSResolver struct {
	store UnixFSStore
	root  unixfsstore.RootCID
}

type traversalState struct {
	root          unixfsstore.RootCID
	currentPath   string
	blockMetadata stargate.BlockMetadata
}

// ResolvePathSegments attempts to resolve a UnixFS path
// On success, it resolves all paths and returns:
// -  a stargate path message for the whole path
// - no unresolved segments
// - a path resolver operating at the end of the path
// On error, all values are be nil except the error value
func (ufsr *UnixFSResolver) ResolvePathSegments(ctx context.Context, path stargate.PathSegments) (*stargate.Path, stargate.PathSegments, stargate.PathResolver, error) {
	state := traversalState{
		blockMetadata: make(stargate.BlockMetadata, 0, len(path)*4),

		root:        ufsr.root,
		currentPath: "",
	}
	for _, segment := range path {
		var err error
		state, err = ufsr.traverseSegment(ctx, state, segment)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return &stargate.Path{
		Segments: path,
		Blocks:   state.blockMetadata,
	}, nil, &UnixFSResolver{store: ufsr.store, root: state.root}, nil
}

func (ufsr *UnixFSResolver) traverseSegment(ctx context.Context, state traversalState, segment string) (traversalState, error) {
	// reject resolution for anything that isn't a directory
	if state.root.Kind != data.Data_Directory && state.root.Kind != data.Data_HAMTShard {
		return traversalState{}, stargate.ErrPathError{state.root.CID, state.currentPath, errors.New("cannot path into a file, must be a directory")}
	}
	// guestimate a maximum size for the path factoring hamts
	pathCids, err := ufsr.store.DirPath(ctx, state.root.CID, state.root.Metadata, segment)
	if err != nil {
		return traversalState{}, err
	}
	if len(pathCids) == 0 {
		return traversalState{}, stargate.ErrPathError{state.root.CID, state.currentPath, fmt.Errorf("no file or folder %s", segment)}
	}
	for _, pathCid := range pathCids[:len(pathCids)-1] {
		state.blockMetadata = append(state.blockMetadata, stargate.BlockMetadatum{
			Link:   pathCid,
			Status: stargate.BlockStatusPresent,
		})
	}
	leaf := pathCids[len(pathCids)-1]
	nextRoot, err := ufsr.store.RootCIDWithMetadata(ctx, leaf, ufsr.root.Metadata)
	if err != nil {
		return traversalState{}, err
	}
	if nextRoot == nil {
		return traversalState{}, stargate.ErrNotFound{Cid: leaf}
	}
	state.root = *nextRoot
	state.currentPath += "/"
	state.currentPath += segment
	return state, nil
}

// UnixFSQueryResolver implements an QueryResolver for the UnixFS domain
type UnixFSQueryResolver struct {
	ctx       context.Context
	query     stargate.Query
	store     UnixFSStore
	root      unixfsstore.RootCID
	fulfilled bool
}

// ResolveQuery returns a resolver to fulfill the DAG part of a UnixFS query after path resolution with the
// given query string.
func (ufsr *UnixFSResolver) ResolveQuery(ctx context.Context, query stargate.Query) (stargate.QueryResolver, error) {
	return &UnixFSQueryResolver{
		ctx:   ctx,
		query: query,
		store: ufsr.store,
		root:  ufsr.root,
	}, nil
}

// Done indicates if a UnixFS query resolution is complete. Since there is only one message for UnixFS query resolution,
// Done is true after a single call to Next
func (ufsqr *UnixFSQueryResolver) Done() bool {
	return ufsqr.fulfilled
}

// Next fulfilles a UnixFS DAG query
// For files:
// the query parameter 'noleaves' will prevent leaves from being sent
// the query parameter 'bytes' will narrow results to a specifc range of bytes in the UnixFS file
func (ufsqr *UnixFSQueryResolver) Next() (*stargate.DAG, error) {
	if ufsqr.fulfilled {
		return nil, stargate.ErrNoMoreMessages{}
	}
	defer func() {
		ufsqr.fulfilled = true
	}()
	switch ufsqr.root.Kind {
	case data.Data_Directory, data.Data_HAMTShard:
		return ufsqr.directoryQuery()
	case data.Data_Raw:
		return &stargate.DAG{
			Ordering: stargate.OrderingBreadthFirst,
			Blocks: stargate.BlockMetadata{
				{
					Link:   ufsqr.root.CID,
					Status: stargate.BlockStatusPresent,
				},
			},
		}, nil
	case data.Data_File:
		return ufsqr.fileQuery()
	default:
		return nil, fmt.Errorf("unsupported file type: %d", ufsqr.root.Kind)
	}

}

func (ufsqr *UnixFSQueryResolver) directoryQuery() (*stargate.DAG, error) {
	cidLayers, err := ufsqr.store.DirLs(ufsqr.ctx, ufsqr.root.CID, ufsqr.root.Metadata)
	if err != nil {
		return nil, err
	}
	totalCids := 1
	for _, cidLayer := range cidLayers {
		totalCids += len(cidLayer)
	}
	blockMetadata := make(stargate.BlockMetadata, 0, totalCids)
	blockMetadata = append(blockMetadata, stargate.BlockMetadatum{
		Link:   ufsqr.root.CID,
		Status: stargate.BlockStatusPresent,
	})
	for _, cidLayer := range cidLayers {
		for _, traversedCID := range cidLayer {
			status := stargate.BlockStatusPresent
			if traversedCID.IsLeaf {
				status = stargate.BlockStatusNotSent
			}
			blockMetadata = append(blockMetadata, stargate.BlockMetadatum{
				Link:   traversedCID.CID,
				Status: status,
			})
		}
	}
	return &stargate.DAG{
		Ordering: stargate.OrderingBreadthFirst,
		Blocks:   blockMetadata,
	}, nil
}

func (ufsqr *UnixFSQueryResolver) fileQuery() (*stargate.DAG, error) {
	cidLayers, err := ufsqr.store.FileAll(ufsqr.ctx, ufsqr.root.CID, ufsqr.root.Metadata)
	if err != nil {
		return nil, err
	}

	var byteCidSets []map[cid.Cid]struct{}
	if bytesParams, ok := ufsqr.query["bytes"]; ok {
		start, end, err := splitBytesParams(bytesParams)
		if err != nil {
			return nil, fmt.Errorf("incorrectly formatted byte param")
		}
		byteCidLayers, err := ufsqr.store.FileByteRange(ufsqr.ctx, ufsqr.root.CID, ufsqr.root.Metadata, start, end)
		if err != nil {
			return nil, err
		}
		byteCidSets = make([]map[cid.Cid]struct{}, len(byteCidLayers))
		for i, byteCidLayer := range byteCidLayers {
			byteCidSet := make(map[cid.Cid]struct{}, len(byteCidLayer))
			for _, traversedCid := range byteCidLayer {
				byteCidSet[traversedCid.CID] = struct{}{}
			}
			byteCidSets[i] = byteCidSet
		}
	}
	sendLeaves := true
	if _, ok := ufsqr.query["noleaves"]; ok {
		sendLeaves = false
	}

	totalCids := 1
	for _, cidLayer := range cidLayers {
		totalCids += len(cidLayer)
	}
	blockMetadata := make(stargate.BlockMetadata, 0, totalCids)
	blockMetadata = append(blockMetadata, stargate.BlockMetadatum{
		Link:   ufsqr.root.CID,
		Status: stargate.BlockStatusPresent,
	})
	for i, cidLayer := range cidLayers {
		for _, traversedCID := range cidLayer {
			status := stargate.BlockStatusPresent
			if byteCidSets != nil {
				if _, ok := byteCidSets[i][traversedCID.CID]; !ok {
					status = stargate.BlockStatusNotSent
				}
			}
			if traversedCID.IsLeaf && !sendLeaves {
				status = stargate.BlockStatusNotSent
			}
			blockMetadata = append(blockMetadata, stargate.BlockMetadatum{
				Link:   traversedCID.CID,
				Status: status,
			})
		}
	}
	return &stargate.DAG{
		Ordering: stargate.OrderingBreadthFirst,
		Blocks:   blockMetadata,
	}, nil
}

func splitBytesParams(bytesParams []string) (uint64, uint64, error) {
	// only use the first
	bytesParam := bytesParams[0]
	parts := strings.Split(bytesParam, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("must be seperated by a single dash")
	}
	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}
