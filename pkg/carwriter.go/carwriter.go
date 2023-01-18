package carwriter

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
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

var log = logging.Logger("stargate-carwriter")

const BufferSize = (1 << 20) * 16

func WriteCar(ctx context.Context, w io.Writer, root cid.Cid, paths stargate.PathSegments, query stargate.Query, appResolver stargate.AppResolver) error {
	header := car.CarHeader{
		Version: 1,
		Roots:   []cid.Cid{root},
	}
	err := car.WriteHeader(&header, w)
	if err != nil {
		return fmt.Errorf("Writing car header: %w", err)
	}
	lsys, resolver, err := appResolver.GetResolver(ctx, root)
	if err != nil {
		return fmt.Errorf("Error loading root resolver: %w", err)
	}
	fmt.Println(paths)
	for len(paths) != 0 {
		var path *stargate.Path
		path, paths, resolver, err = resolver.ResolvePathSegments(ctx, paths)
		if err != nil {
			return fmt.Errorf("Resolving path segments: %w", err)
		}
		err = writeStarGateMessageAndBlocks(ctx, w, stargate.StarGateMessage{
			Kind: stargate.KindPath,
			Path: path,
		}, lsys)
		if err != nil {
			return fmt.Errorf("Encoding stargate message and blocks: %w", err)
		}
	}
	queryResolver, err := resolver.ResolveQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("Resolving Query: %w", err)
	}
	for !queryResolver.Done() {
		dag, err := queryResolver.Next()
		if err != nil {
			return fmt.Errorf("Resolving Query Step: %w", err)
		}
		err = writeStarGateMessageAndBlocks(ctx, w, stargate.StarGateMessage{
			Kind: stargate.KindDAG,
			DAG:  dag,
		}, lsys)
		if err != nil {
			return fmt.Errorf("Encoding stargate message and blocks: %w", err)
		}
	}
	return nil
}

type bytesReader interface {
	Bytes() []byte
}

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
