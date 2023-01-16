package sql

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql/fielddef"
)

type FileLink struct {
	RootCID  cid.Cid
	Metadata fielddef.SqlBytes
	CID      cid.Cid
	Depth    uint64
	Leaf     bool
	ByteMin  uint64
	ByteMax  uint64
}

var fileLinksOrder = []string{"RootCID", "Metadata", "CID", "Depth", "Leaf", "ByteMin", "ByteMax"}

func InsertFileLink(ctx context.Context, db Transactable, fileLink *FileLink) error {
	return fielddef.Insert(ctx, db, "FileLinks", fileLinksOrder, fileLinkFields(fileLink))
}

func fileLinkFields(fileLink *FileLink) map[string]fielddef.FieldDefinition {
	return map[string]fielddef.FieldDefinition{
		"RootCID":  &fielddef.CidFieldDef{F: &fileLink.RootCID},
		"Metadata": &fielddef.BytesFieldDef{F: &fileLink.Metadata},
		"CID":      &fielddef.CidFieldDef{F: &fileLink.CID},
		"Depth":    &fielddef.FieldDef{F: &fileLink.Depth},
		"Leaf":     &fielddef.FieldDef{F: &fileLink.Leaf},
		"ByteMin":  &fielddef.FieldDef{F: &fileLink.ByteMin},
		"ByteMax":  &fielddef.FieldDef{F: &fileLink.ByteMax},
	}
}

func fileLinkQuery(ctx context.Context, db Transactable, whereClause string, whereParams ...any) ([][]cid.Cid, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(fileQuery, whereClause), whereParams...)
	if err != nil {
		return nil, err
	}
	cidDepths := make([][]cid.Cid, 0, 16)
	for rows.Next() {
		var c cid.Cid
		var depth uint64
		err := fielddef.Scan(rows, []string{"CID", "Depth"}, map[string]fielddef.FieldDefinition{
			"CID":   &fielddef.CidFieldDef{F: &c},
			"Depth": &fielddef.FieldDef{F: &depth},
		})
		if err != nil {
			return nil, err
		}
		for uint64(len(cidDepths)) <= depth {
			cidDepths = append(cidDepths, make([]cid.Cid, 0, 16))
		}
		cidDepths[depth] = append(cidDepths[depth], c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cidDepths, nil
}

func FileByteRange(ctx context.Context, db Transactable, root cid.Cid, metadata fielddef.SqlBytes, min uint64, max uint64) ([][]cid.Cid, error) {
	return fileLinkQuery(ctx, db, "WHERE RootCID = ? AND Metadata = ? AND ByteMin < ? AND ByteMax > ?", root.Bytes(), metadata.Bytes(), max, min)
}

var fileQuery string = "SELECT DISTINCT CID, Depth FROM FileLinks %s ORDER BY Depth ASC"

func FileAll(ctx context.Context, db Transactable, root cid.Cid, metadata fielddef.SqlBytes) ([][]cid.Cid, error) {
	return fileLinkQuery(ctx, db, "WHERE RootCID = ? AND Metadata = ?", root.Bytes(), metadata.Bytes())
}
