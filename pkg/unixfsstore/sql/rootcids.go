package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/pkg/unixfsstore"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql/fielddef"
)

func InsertRootCID(ctx context.Context, db Transactable, rootCID unixfsstore.RootCID) error {
	return fielddef.Insert(ctx, db, "RootCIDs", []string{"CID", "Kind", "Metadata"}, map[string]fielddef.FieldDefinition{
		"CID":      &fielddef.CidFieldDef{F: &rootCID.CID},
		"Kind":     &fielddef.FieldDef{F: &rootCID.Kind},
		"Metadata": &fielddef.BytesFieldDef{F: (*fielddef.SqlBytes)(&rootCID.Metadata)},
	})
}

var getByCID string = "SELECT Kind, Metadata FROM RootCIDs WHERE CID = ?"

func RootCID(ctx context.Context, db Transactable, root cid.Cid) ([]unixfsstore.RootCID, error) {
	rows, err := db.QueryContext(ctx, getByCID, root.Bytes())
	if err != nil {
		return nil, err
	}
	var rootCIDs []unixfsstore.RootCID
	for rows.Next() {
		rootCID := unixfsstore.RootCID{
			CID: root,
		}
		err := fielddef.Scan(rows, []string{"Kind", "Metadata"}, map[string]fielddef.FieldDefinition{
			"Kind":     &fielddef.FieldDef{F: &rootCID.Kind},
			"Metadata": &fielddef.BytesFieldDef{F: (*fielddef.SqlBytes)(&rootCID.Metadata)},
		})
		if err != nil {
			return nil, err
		}
		rootCIDs = append(rootCIDs, rootCID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return rootCIDs, nil
}

var getByCIDAndMetadata string = "SELECT Kind FROM RootCIDs WHERE CID = ? AND Metadata = ?"

func RootCIDWithMetadata(ctx context.Context, db Transactable, root cid.Cid, metadata []byte) (*unixfsstore.RootCID, error) {
	row := db.QueryRowContext(ctx, getByCIDAndMetadata, root.Bytes(), fielddef.SqlBytes(metadata).Bytes())
	kind := int64(-1)
	err := fielddef.Scan(row, []string{"Kind"}, map[string]fielddef.FieldDefinition{
		"Kind": &fielddef.FieldDef{F: &kind},
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &unixfsstore.RootCID{
		CID:      root,
		Kind:     kind,
		Metadata: metadata,
	}, nil
}
