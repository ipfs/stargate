package stargate

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/storage"
)

type AppResolver interface {
	GetResolver(root cid.Cid) (storage.StreamingReadableStorage, Resolver)
}

type Resolver interface {
	ResolvePathSegment(path string) (BlockMetadata, Resolver, error)
	ResolveQuery(query string) (BlockMetadata, error)
}
