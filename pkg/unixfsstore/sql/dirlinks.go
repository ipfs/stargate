package sql

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql/fielddef"
)

type DirLink struct {
	RootCID  cid.Cid
	Metadata fielddef.SqlBytes
	CID      cid.Cid
	Depth    uint64
	Leaf     bool
	SubPath  string
}

var dirLinksOrder = []string{"RootCID", "Metadata", "CID", "Depth", "Leaf", "SubPath"}

func InsertDirLink(ctx context.Context, db Transactable, dirLink *DirLink) error {
	return fielddef.Insert(ctx, db, "DirLinks", dirLinksOrder, dirLinkFields(dirLink))
}

func dirLinkFields(dirLink *DirLink) map[string]fielddef.FieldDefinition {
	return map[string]fielddef.FieldDefinition{
		"RootCID":  &fielddef.CidFieldDef{F: &dirLink.RootCID},
		"Metadata": &fielddef.BytesFieldDef{F: &dirLink.Metadata},
		"CID":      &fielddef.CidFieldDef{F: &dirLink.CID},
		"Depth":    &fielddef.FieldDef{F: &dirLink.Depth},
		"Leaf":     &fielddef.FieldDef{F: &dirLink.Leaf},
		"SubPath":  &fielddef.FieldDef{F: &dirLink.SubPath},
	}
}

var pathQuery string = "SELECT CID FROM DirLinks WHERE RootCID = ? AND Metadata = ? AND SubPath = ? ORDER BY Depth ASC"

func DirPath(ctx context.Context, db Transactable, root cid.Cid, metadata fielddef.SqlBytes, path string) ([]cid.Cid, error) {
	rows, err := db.QueryContext(ctx, pathQuery, root.Bytes(), metadata.Bytes(), path)
	if err != nil {
		return nil, err
	}
	cids := make([]cid.Cid, 0, 16)
	for rows.Next() {
		var c cid.Cid
		err := fielddef.Scan(rows, []string{"CID"}, map[string]fielddef.FieldDefinition{
			"CID": &fielddef.CidFieldDef{F: &c},
		})
		if err != nil {
			return nil, err
		}
		cids = append(cids, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cids, nil
}

var lsQuery string = "SELECT DISTINCT CID, Depth, Leaf FROM DirLinks WHERE RootCID = ? AND Metadata = ? ORDER BY Depth ASC"

func DirLs(ctx context.Context, db Transactable, root cid.Cid, metadata fielddef.SqlBytes) ([][]unixfsstore.TraversedCID, error) {
	rows, err := db.QueryContext(ctx, lsQuery, root.Bytes(), metadata.Bytes())
	if err != nil {
		return nil, err
	}
	cidDepths := make([][]unixfsstore.TraversedCID, 0, 16)
	for rows.Next() {
		var c unixfsstore.TraversedCID
		var depth uint64
		err := fielddef.Scan(rows, []string{"CID", "Depth", "Leaf"}, map[string]fielddef.FieldDefinition{
			"CID":   &fielddef.CidFieldDef{F: &c.CID},
			"Depth": &fielddef.FieldDef{F: &depth},
			"Leaf":  &fielddef.FieldDef{F: &c.IsLeaf},
		})
		if err != nil {
			return nil, err
		}
		for uint64(len(cidDepths)) <= depth {
			cidDepths = append(cidDepths, make([]unixfsstore.TraversedCID, 0, 16))
		}
		cidDepths[depth] = append(cidDepths[depth], c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cidDepths, nil
}
