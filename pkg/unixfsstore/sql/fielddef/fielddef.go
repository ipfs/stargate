package fielddef

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
)

type FieldDefinition interface {
	FieldPtr() interface{}
	Marshall() (interface{}, error)
	Unmarshall() error
}

type FieldDef struct {
	F interface{}
}

var _ FieldDefinition = (*FieldDef)(nil)

func (fd *FieldDef) FieldPtr() interface{} {
	return fd.F
}

func (fd *FieldDef) Marshall() (interface{}, error) {
	return fd.F, nil
}

func (fd *FieldDef) Unmarshall() error {
	return nil
}

type CidFieldDef struct {
	Marshalled []byte
	F          *cid.Cid
}

func (fd *CidFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *CidFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.Bytes(), nil
}

func (fd *CidFieldDef) Unmarshall() error {

	_, c, err := cid.CidFromBytes(fd.Marshalled)
	if err != nil {
		return fmt.Errorf("parsing CID from bytes: %w", err)
	}

	*fd.F = c
	return nil
}

type SqlBytes []byte

func (m SqlBytes) Bytes() []byte {
	if m == nil {
		return []byte{}
	}
	return m
}

type BytesFieldDef struct {
	Marshalled []byte
	F          *SqlBytes
}

func (fd *BytesFieldDef) FieldPtr() interface{} {
	return &fd.Marshalled
}

func (fd *BytesFieldDef) Marshall() (interface{}, error) {
	if fd.F == nil {
		return nil, nil
	}
	return fd.F.Bytes(), nil
}

func (fd *BytesFieldDef) Unmarshall() error {
	if len(fd.Marshalled) == 0 && fd.Marshalled != nil {
		*fd.F = nil
		return nil
	}
	*fd.F = fd.Marshalled
	return nil
}

type Scannable interface {
	Scan(dest ...interface{}) error
}

func Scan(row Scannable, fieldOrder []string, def map[string]FieldDefinition) error {
	// For each field
	dest := []interface{}{}
	for _, name := range fieldOrder {
		fieldDef := def[name]
		// Get a pointer to the field that will receive the scanned value
		dest = append(dest, fieldDef.FieldPtr())
	}

	// Scan the row into each pointer
	err := row.Scan(dest...)
	if err != nil {
		return fmt.Errorf("scanning deal row: %w", err)
	}

	// For each field
	for name, fieldDef := range def {
		// Unmarshall the scanned value into deal object
		err := fieldDef.Unmarshall()
		if err != nil {
			return fmt.Errorf("unmarshalling db field %s: %s", name, err)
		}
	}
	return nil
}

type Executable interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func Insert(ctx context.Context, db Executable, table string, fieldOrder []string, def map[string]FieldDefinition) error {
	// For each field
	values := []interface{}{}
	placeholders := make([]string, 0, len(values))
	for _, name := range fieldOrder {
		// Add a placeholder "?"
		fieldDef := def[name]
		placeholders = append(placeholders, "?")

		// Marshall the field into a value that can be stored in the database
		v, err := fieldDef.Marshall()
		if err != nil {
			return err
		}
		values = append(values, v)
	}

	// Execute the INSERT
	qry := "INSERT INTO " + table + " (" + strings.Join(fieldOrder, ", ") + ") "
	qry += "VALUES (" + strings.Join(placeholders, ",") + ")"
	_, err := db.ExecContext(ctx, qry, values...)
	return err
}
