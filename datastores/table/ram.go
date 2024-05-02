package table

import (
	"github.com/dsnet/golib/memfile"
	"github.com/ohzqq/hare/datastores/store"
)

type MemFile struct {
	*memfile.File
}

func Mem(d []byte) *MemFile {
	return &MemFile{
		File: memfile.New(d),
	}
}

func (m *MemFile) Close() error { return nil }

type Ram struct {
	path string
	ext  string
	*store.Store
}

func NewRam(tables map[string][]byte) (*Ram, error) {
	ram := &Ram{
		Store: store.New(),
	}

	for tableName, data := range tables {
		err := ram.Store.CreateTable(tableName, Mem(data))
		if err != nil {
			return nil, err
		}
	}
	return ram, nil
}

// Close closes the datastore.
func (ram *Ram) Close() error {
	for _, tableFile := range ram.Tables {
		if err := tableFile.Close(); err != nil {
			return err
		}
	}

	ram.Tables = nil

	return nil
}

// CreateTable takes a table name, creates a new disk
// file, and adds it to the map of tables in the
// datastore.
func (ram *Ram) CreateTable(tableName string) error {
	err := ram.Store.CreateTable(tableName, Mem([]byte{}))
	if err != nil {
		return err
	}

	return nil
}

// InsertRec takes a table name, a record id, and a byte array and adds
// the record to the table.

// ReadRec takes a table name and an id, reads the record from the
// table, and returns a populated byte array.

// RemoveTable takes a table name and deletes that table file from the
// disk.
func (ram *Ram) RemoveTable(tableName string) error {
	tableFile, err := ram.GetTableFile(tableName)
	if err != nil {
		return err
	}

	tableFile.Close()

	delete(ram.Tables, tableName)

	return nil
}

// TableExists takes a table name and returns a bool indicating
// whether or not the table exists in the datastore.

// TableNames returns an array of table names.

// UpdateRec takes a table name, a record id, and a byte array and updates
// the table record with that id.
