package stargate

import (
	_ "embed"

	"github.com/ipfs/go-cid"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
)

type BlockStatus string

const (
	// Present means the linked block was present on this machine, and is included
	// in this message
	BlockStatusPresent BlockStatus = "Present"
	// NotSent means the linked block was present on this machine, but not sent
	// - it needs to be fetched elsewhere
	BlockStatusNotSent BlockStatus = "NotSent"
	// Missing means I did not have the linked block, so I skipped over this part
	// of the traversal
	BlockStatusMissing BlockStatus = "Missing"
	// Duplicate means the linked block was encountered, but we already have traversed it
	// so we're not traversing it again -- the block has likely already been transmitted
	BlockStatusDuplicate BlockStatus = "Duplicate"
)

// Metadata for each "link" in the DAG being communicated, each block gets one of
// these and missing blocks also get one
type BlockMetadatum struct {
	Link   cid.Cid
	Status BlockStatus
}

type BlockMetadata []BlockMetadatum

type Path struct {
	// name of this path segment
	Segments []string
	// CIDs required, in order, to verify this segment of the path
	Blocks BlockMetadata
}

type Ordering string

const (
	// Depthfirst indicates blocks will be transmitted depth first
	OrderingDepthFirst Ordering = "DepthFirst"
	// BreadthFirst indicates blocks will be breadth depth first
	OrderingBreadthFirst Ordering = "BreadthFirst"
)

type DAG struct {
	Ordering Ordering
	Blocks   BlockMetadata
}

type Kind string

const (
	// Path indicates a pathing sequence
	KindPath Kind = "Path"
	// DAG indicates a DAG block
	KindDAG Kind = "DAG"
)

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
