package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/stargate/pkg/unixfsstore/sql/fielddef"
)

func InsertRootCID(ctx context.Context, db Transactable, root cid.Cid, kind int64, metadata fielddef.SqlBytes) error {
	return fielddef.Insert(ctx, db, "RootCIDs", []string{"CID", "Kind", "Metadata"}, map[string]fielddef.FieldDefinition{
		"CID":      &fielddef.CidFieldDef{F: &root},
		"Kind":     &fielddef.FieldDef{F: &kind},
		"Metadata": &fielddef.BytesFieldDef{F: &metadata},
	})
}

var getByCID string = "SELECT Kind FROM RootCIDs WHERE CID = ? AND Metadata = ?"

func RootCID(ctx context.Context, db Transactable, root cid.Cid, metadata fielddef.SqlBytes) (bool, int64, error) {
	row := db.QueryRowContext(ctx, getByCID, root.Bytes(), metadata.Bytes())
	kind := int64(-1)
	err := fielddef.Scan(row, []string{"Kind"}, map[string]fielddef.FieldDefinition{
		"Kind": &fielddef.FieldDef{F: &kind},
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, -1, nil
		}
		return false, -1, err
	}
	return true, kind, nil
}
