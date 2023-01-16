package traversal

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/hamt"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
)

// DiscoverRoots scans all keys in a store and finds UnixFS roots among them
func DiscoverRoots(ctx context.Context, incoming <-chan cid.Cid, ls *linking.LinkSystem) ([]cid.Cid, error) {
	roots := make(map[cid.Cid]struct{})
	nonRoots := make(map[cid.Cid]struct{})
	for {
		select {
		case next, ok := <-incoming:
			if !ok {
				rootsResult := make([]cid.Cid, 0, len(roots))
				for root := range roots {
					rootsResult = append(rootsResult, root)
				}
				return rootsResult, nil
			}

			// we only care about protobuf nodes
			var nonRootChildren []cid.Cid
			switch multicodec.Code(next.Type()) {
			case multicodec.DagPb:
				nd, err := ls.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{next}, dagpb.Type.PBNode)
				if err != nil {
					return nil, fmt.Errorf("malformed blockstore cid %s: %w", next.String(), err)
				}
				pbnd, ok := nd.(dagpb.PBNode)
				if !ok {
					return nil, fmt.Errorf("malformed blockstore cid %s: %w", next.String(), hamt.ErrNotProtobuf)
				}
				// if no data field, ignore
				if !pbnd.FieldData().Exists() {
					continue
				}
				// if not UnixFS data, ignore
				ufsdata, err := data.DecodeUnixFSData(pbnd.FieldData().Must().Bytes())
				if err != nil {
					continue
				}
				// ok, it's a unixfsnode, so we may want to add as root
				// record relevant non-root children
				switch ufsdata.DataType.Int() {
				case data.Data_File:
					// for a regular file, all children are now non-root children
					iter := pbnd.Links.Iterator()
					for !iter.Done() {
						_, lnk := iter.Next()
						nonRootChildren = append(nonRootChildren, lnk.Hash.Link().(cidlink.Link).Cid)
					}
				case data.Data_HAMTShard:
					// for a hamt directory, all children that are not value nodes are non root children
					iter := pbnd.Links.Iterator()
					maxPadLen := maxPadLength(ufsdata)
					for !iter.Done() {
						_, lnk := iter.Next()
						isValue, err := isValueLink(lnk, maxPadLen)
						if err != nil {
							return nil, err
						}
						if !isValue {
							nonRootChildren = append(nonRootChildren, lnk.Hash.Link().(cidlink.Link).Cid)
						}
					}
				default:
					// all other unixfs types do not have non-root children
				}
			case multicodec.Raw:
				// raw may be a root, but it has no children
			default:
				// not raw or dabpb, ignore
				continue
			}

			for _, child := range nonRootChildren {
				_, isRoot := roots[child]
				if isRoot {
					delete(roots, child)
				}
				nonRoots[child] = struct{}{}
			}
			_, isNonRoot := nonRoots[next]
			if !isNonRoot {
				roots[next] = struct{}{}
			}
		}
	}
}
