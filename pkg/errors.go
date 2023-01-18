package stargate

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

type ErrNotFound struct {
	Cid cid.Cid
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("Unable to find CID: %s", e.Cid)
}

type ErrPathError struct {
	Cid  cid.Cid
	Path string
	Err  error
}

func (e ErrPathError) Unwrap() error {
	return e.Err
}

func (e ErrPathError) Error() string {
	return fmt.Sprintf("path traversal error at %s/%s: %s", e.Cid, e.Path, e.Err)
}

type ErrNoMoreMessages struct{}

func (e ErrNoMoreMessages) Error() string {
	return fmt.Sprintf("query resolution already complete")
}
