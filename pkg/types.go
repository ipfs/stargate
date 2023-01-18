package stargate

import (
	_ "embed"

	"github.com/ipfs/go-cid"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
)

// BlockStatus indicates information about what is being done with a given block in a request
type BlockStatus string

const (
	// BlockStatusPresent means the linked block was present on this machine, and is included
	// in this message
	BlockStatusPresent BlockStatus = "Present"
	// BlockStatusNotSent means the linked block was present on this machine, but not sent
	// - it needs to be fetched elsewhere
	BlockStatusNotSent BlockStatus = "NotSent"
	// BlockStatusMissing means I did not have the linked block, so I skipped over this part
	// of the traversal
	BlockStatusMissing BlockStatus = "Missing"
	// BlockStatusDuplicate means the linked block was encountered, but we already have traversed it
	// so we're not traversing it again -- the block has likely already been transmitted
	BlockStatusDuplicate BlockStatus = "Duplicate"
)

// BlockMetadatum is metadata for a single block
type BlockMetadatum struct {
	Link   cid.Cid
	Status BlockStatus
}

// BlockMetadata is metadata for each "link" in the DAG being communicated, each block gets one of
// these and missing blocks also get one
type BlockMetadata []BlockMetadatum

// Path is a StarGate message that provides information about resolution of a path
type Path struct {
	// name of this path segment
	Segments []string
	// CIDs required, in order, to verify this segment of the path
	Blocks BlockMetadata
}

// Ordering is a traversal order for transmitting blocks
type Ordering string

const (
	// OrderingDepthFirst indicates blocks will be transmitted depth first
	OrderingDepthFirst Ordering = "DepthFirst"
	// OrderingBreadthFirst indicates blocks will be breadth depth first
	OrderingBreadthFirst Ordering = "BreadthFirst"
)

// Path is a StarGate message that provides information about resolution of the DAG at the end of a query
type DAG struct {
	Ordering Ordering
	Blocks   BlockMetadata
}

// Kind indicates whether a generic StarGate message is for a Path or a DAG
type Kind string

const (
	// KindPath indicates a pathing sequence
	KindPath Kind = "Path"
	// KindDAG indicates a DAG block
	KindDAG Kind = "DAG"
)

// StarGateMessage is a complete StarGate message ahead of a block sequence
type StarGateMessage struct {
	Kind Kind
	Path *Path
	DAG  *DAG
}

//go:embed types.ipldsch
var embedSchema []byte

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	if err := BindnodeRegistry.RegisterType((*StarGateMessage)(nil), string(embedSchema), "StarGateMessage"); err != nil {
		panic(err.Error())
	}
}
