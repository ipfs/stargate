package carwriter

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	stargate "github.com/ipfs/stargate/pkg"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

// WriteCar traverses a StarGate query using a resolver to write StarGate CAR response to the given writer
func WriteCar(ctx context.Context, w io.Writer, root cid.Cid, paths stargate.PathSegments, query stargate.Query, appResolver stargate.AppResolver) error {
	// write CAR header
	header := car.CarHeader{
		Version: 1,
		Roots:   []cid.Cid{root},
	}
	err := car.WriteHeader(&header, w)
	if err != nil {
		return fmt.Errorf("writing car header: %w", err)
	}
	// resolve root
	lsys, resolver, err := appResolver.GetResolver(ctx, root)
	if err != nil {
		return fmt.Errorf("error loading root resolver: %w", err)
	}
	// resolve all path segments
	for len(paths) != 0 {
		var path *stargate.Path
		path, paths, resolver, err = resolver.ResolvePathSegments(ctx, paths)
		if err != nil {
			return fmt.Errorf("resolving path segments: %w", err)
		}
		err = writeStarGateMessageAndBlocks(ctx, w, stargate.StarGateMessage{
			Kind: stargate.KindPath,
			Path: path,
		}, lsys)
		if err != nil {
			return fmt.Errorf("encoding stargate message and blocks: %w", err)
		}
	}
	// resolve query
	queryResolver, err := resolver.ResolveQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("resolving Query: %w", err)
	}
	for !queryResolver.Done() {
		dag, err := queryResolver.Next()
		if err != nil {
			return fmt.Errorf("resolving Query Step: %w", err)
		}
		err = writeStarGateMessageAndBlocks(ctx, w, stargate.StarGateMessage{
			Kind: stargate.KindDAG,
			DAG:  dag,
		}, lsys)
		if err != nil {
			return fmt.Errorf("encoding stargate message and blocks: %w", err)
		}
	}
	return nil
}

type bytesReader interface {
	Bytes() []byte
}

// writeStarGateMessageAndBlocks serializes a StarGate message and its associate blocks
func writeStarGateMessageAndBlocks(ctx context.Context, w io.Writer, msg stargate.StarGateMessage, lsys *ipld.LinkSystem) error {
	raw, err := stargate.BindnodeRegistry.TypeToBytes(&msg, dagcbor.Encode)
	if err != nil {
		return err
	}
	messageLink, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}.Sum(raw)
	if err != nil {
		return err
	}
	err = util.LdWrite(w, messageLink.Bytes(), raw)
	if err != nil {
		return err
	}
	var blockMetadata stargate.BlockMetadata
	if msg.Kind == stargate.KindPath {
		blockMetadata = msg.Path.Blocks
	} else {
		blockMetadata = msg.DAG.Blocks
	}
	for _, blockMetadatum := range blockMetadata {
		if blockMetadatum.Status == stargate.BlockStatusPresent {
			reader, err := lsys.StorageReadOpener(linking.LinkContext{
				Ctx: ctx,
			}, cidlink.Link{Cid: blockMetadatum.Link})
			if err != nil {
				return err
			}
			var data []byte
			if br, ok := reader.(bytesReader); ok {
				data = br.Bytes()
			} else {
				data, err = io.ReadAll(reader)
				if err != nil {
					return err
				}
			}
			err = util.LdWrite(w, blockMetadatum.Link.Bytes(), data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
