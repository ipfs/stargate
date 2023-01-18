package unixfsstore

import (
	"github.com/ipfs/go-cid"
)

type RootCID struct {
	CID      cid.Cid
	Kind     int64
	Metadata []byte
}

type TraversedCID struct {
	CID    cid.Cid
	IsLeaf bool
}
