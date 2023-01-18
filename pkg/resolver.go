package stargate

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

// Query is a URL query string to pass into stargate
type Query map[string][]string

// PathSegments are the path segments of a URL
type PathSegments []string

// AppResolver finds the root of a dag and returns an associated blockstore to use serving the request
type AppResolver interface {
	// GetResolver attempts to resolve starting from the given root. It returns a linksystem to load blocks from
	// and a resolver for the query
	GetResolver(ctx context.Context, root cid.Cid) (*ipld.LinkSystem, PathResolver, error)
}

// PathResolver resolves the URL path
type PathResolver interface {
	// ResolvePathSegments attempts to resolve a path
	// On success, an implementation should:
	// - resolve at least one path segment
	// - return:
	//   - a valid stargate path message
	//   - any unresolved segments
	//   - a resolver operating whose root is at the end of the resolved portion of the path
	// On error, all values should be nil except the error value
	ResolvePathSegments(ctx context.Context, path PathSegments) (*Path, PathSegments, PathResolver, error)

	// ResolverQuery returns a resolver to fulfill the remaining portion of a request after path resolution with the
	// given query string.
	ResolveQuery(ctx context.Context, query Query) (QueryResolver, error)
}

// QueryResolver produces one or more stargate DAG messages to fulfill the request at the end of the path
type QueryResolver interface {
	// Next returns the next message
	Next() (*DAG, error)
	// done indicates where the query is fully resolved or there are more messages
	Done() bool
}
