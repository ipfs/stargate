package traversal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/hamt"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type UnixFSVisitor interface {
	OnPath(ctx context.Context, root cid.Cid, path string, cids []cid.Cid) error
	OnFileRange(ctx context.Context, root cid.Cid, cid cid.Cid, depth int, byteMin uint64, byteMax uint64, leaf bool) error
	OnRoot(ctx context.Context, root cid.Cid, kind int64) error
}

type recursiveVisitor struct {
	UnixFSVisitor
	lsys *ipld.LinkSystem
}

func (rv *recursiveVisitor) OnPath(ctx context.Context, root cid.Cid, path string, cids []cid.Cid) error {
	err := rv.UnixFSVisitor.OnPath(ctx, root, path, cids)
	if err != nil {
		return err
	}
	return IterateUnixFSNode(ctx, cids[len(cids)-1], rv.lsys, rv)
}

func RecursiveVisitor(visitor UnixFSVisitor, lsys *ipld.LinkSystem) UnixFSVisitor {
	return &recursiveVisitor{UnixFSVisitor: visitor, lsys: lsys}
}

type interateFunc func(context.Context, cid.Cid, dagpb.PBNode, data.UnixFSData, *ipld.LinkSystem, UnixFSVisitor) error

func noopIterate(context.Context, cid.Cid, dagpb.PBNode, data.UnixFSData, *ipld.LinkSystem, UnixFSVisitor) error {
	return nil
}

var interateFuncs = map[int64]interateFunc{
	data.Data_File:      IterateFileLinks,
	data.Data_Metadata:  noopIterate,
	data.Data_Raw:       noopIterate,
	data.Data_Symlink:   noopIterate,
	data.Data_Directory: IterateDirLinks,
	data.Data_HAMTShard: IterateHAMTDirLinks,
}

func IterateUnixFSNode(ctx context.Context, root cid.Cid, lsys *ipld.LinkSystem, visitor UnixFSVisitor) error {
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{root}, dagpb.Type.PBNode)
	if err != nil {
		return err
	}
	pbnd, ok := nd.(dagpb.PBNode)
	if !ok {
		return hamt.ErrNotProtobuf
	}
	if !pbnd.FieldData().Exists() {
		return hamt.ErrNotUnixFSNode
	}
	ufsdata, err := data.DecodeUnixFSData(pbnd.FieldData().Must().Bytes())
	if err != nil {
		return err
	}
	dt := ufsdata.DataType.Int()
	if err := visitor.OnRoot(ctx, root, dt); err != nil {
		return err
	}
	return interateFuncs[dt](ctx, root, pbnd, ufsdata, lsys, visitor)
}

// NewUnixFSHAMTShard attempts to construct a UnixFSHAMTShard node from the base protobuf node plus
// a decoded UnixFSData structure
func IterateHAMTDirLinks(ctx context.Context, root cid.Cid, substrate dagpb.PBNode, data data.UnixFSData, lsys *ipld.LinkSystem, visitor UnixFSVisitor) error {
	return iterateHAMTDirLinks(ctx, root, substrate, data, lsys, nil, visitor)
}

func iterateHAMTDirLinks(ctx context.Context, root cid.Cid, substrate dagpb.PBNode, ufsdata data.UnixFSData, lsys *ipld.LinkSystem, cidsSoFar []cid.Cid, visitor UnixFSVisitor) error {
	_, err := hamt.NewUnixFSHAMTShard(ctx, substrate, ufsdata, lsys)
	if err != nil {
		return err
	}
	maxPadLen := maxPadLength(ufsdata)
	itr := substrate.FieldLinks().Iterator()
	st := stringTransformer{maxPadLen: maxPadLen}
	for !itr.Done() {
		_, next := itr.Next()
		isValue, err := isValueLink(next, maxPadLen)
		if err != nil {
			return err
		}
		if isValue {
			if next.FieldName().Exists() {
				name := st.transformNameNode(next.FieldName().Must())
				onPathCids := make([]cid.Cid, 0, len(cidsSoFar)+1)
				// copy array before handing off, so further modifications do not affect result
				for _, c := range cidsSoFar {
					onPathCids = append(onPathCids, c)
				}
				onPathCids = append(onPathCids, next.FieldHash().Link().(cidlink.Link).Cid)
				if err := visitor.OnPath(ctx, root, name.String(), onPathCids); err != nil {
					return err
				}
			}
			continue
		}
		nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, next.FieldHash().Link(), dagpb.Type.PBNode)

		pbnd, ok := nd.(dagpb.PBNode)
		if !ok {
			return hamt.ErrNotProtobuf
		}
		if !pbnd.FieldData().Exists() {
			return hamt.ErrNotUnixFSNode
		}
		nextData, err := data.DecodeUnixFSData(pbnd.FieldData().Must().Bytes())
		if err != nil {
			return err
		}
		if err := iterateHAMTDirLinks(ctx, root, pbnd, nextData, lsys, append(cidsSoFar, next.FieldHash().Link().(cidlink.Link).Cid), visitor); err != nil {
			return err
		}
	}
	return nil
}

type stringTransformer struct {
	maxPadLen int
}

func (s stringTransformer) transformNameNode(nd dagpb.String) dagpb.String {
	nb := dagpb.Type.String.NewBuilder()
	err := nb.AssignString(nd.String()[s.maxPadLen:])
	if err != nil {
		return nil
	}
	return nb.Build().(dagpb.String)
}

func maxPadLength(nd data.UnixFSData) int {
	return len(fmt.Sprintf("%X", nd.FieldFanout().Must().Int()-1))
}

func isValueLink(pbLink dagpb.PBLink, maxPadLen int) (bool, error) {
	if !pbLink.FieldName().Exists() {
		return false, hamt.ErrMissingLinkName
	}
	name := pbLink.FieldName().Must().String()
	if len(name) < maxPadLen {
		return false, hamt.ErrInvalidLinkName{name}
	}
	if len(name) == maxPadLen {
		return false, nil
	}
	return true, nil
}

func IterateDirLinks(ctx context.Context, root cid.Cid, substrate dagpb.PBNode, data data.UnixFSData, lsys *ipld.LinkSystem, visitor UnixFSVisitor) error {
	dir, err := directory.NewUnixFSBasicDir(ctx, substrate, data, lsys)
	if err != nil {
		return err
	}
	iter := dir.(directory.UnixFSBasicDir).Iterator()
	for !iter.Done() {
		path, link := iter.Next()
		if err := visitor.OnPath(ctx, root, path.String(), []cid.Cid{link.Link().(cidlink.Link).Cid}); err != nil {
			return err
		}
	}
	return nil
}

func IterateFileLinks(ctx context.Context, root cid.Cid, substrate dagpb.PBNode, data data.UnixFSData, lsys *ipld.LinkSystem, visitor UnixFSVisitor) error {
	return iterateFileLinks(ctx, root, substrate, data, lsys, 0, 0, visitor)
}

func iterateFileLinks(ctx context.Context, root cid.Cid, substrate dagpb.PBNode, ufsdata data.UnixFSData, lsys *ipld.LinkSystem, bytesOffset uint64, depth int, visitor UnixFSVisitor) error {
	iter := substrate.Links.Iterator()
	for !iter.Done() {
		idx, next := iter.Next()
		nextCid := next.Hash.Link().(cidlink.Link).Cid
		var nextSize uint64
		var leaf bool
		if nextCid.Prefix().Codec == cid.Raw {
			if !next.Tsize.Exists() {
				return errors.New("missing t-size")
			}
			nextSize = uint64(next.Tsize.Must().Int())
			leaf = true
		} else {
			nextSize = uint64(ufsdata.BlockSizes.Lookup(idx).Int())
			nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, next.Hash.Link(), dagpb.Type.PBNode)
			if err != nil {
				return err
			}
			pbnd, ok := nd.(dagpb.PBNode)
			if !ok {
				return hamt.ErrNotProtobuf
			}
			if !pbnd.FieldData().Exists() {
				return hamt.ErrNotUnixFSNode
			}
			nextData, err := data.DecodeUnixFSData(pbnd.FieldData().Must().Bytes())
			if err != nil {
				return err
			}
			switch nextData.DataType.Int() {
			case data.Data_Raw:
				leaf = true
			case data.Data_File:
				if err := iterateFileLinks(ctx, root, pbnd, nextData, lsys, bytesOffset, depth+1, visitor); err != nil {
					return err
				}
			default:
				return data.ErrInvalidDataType{nextData.DataType.Int()}
			}
		}
		if err := visitor.OnFileRange(ctx, root, nextCid, depth, bytesOffset, bytesOffset+nextSize, leaf); err != nil {
			return err
		}
		bytesOffset += nextSize
	}
	return nil
}
