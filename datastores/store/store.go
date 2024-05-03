package store

import (
	"github.com/ohzqq/hare/dberr"
)

type Store struct {
	Tables map[string]*Table
}

func New() *Store {
	return &Store{Tables: make(map[string]*Table)}
}

// CreateTable takes a table name, creates a new disk
// file, and adds it to the map of tables in the
// datastore.
func (store *Store) CreateTable(tableName string, table TableFile) error {
	if store.TableExists(tableName) {
		return dberr.ErrTableExists
	}

	tableFile, err := NewTable(table)
	if err != nil {
		return err
	}

	store.Tables[tableName] = tableFile

	return nil
}

// DeleteRec takes a table name and a record id and deletes
// the associated record.
func (store *Store) DeleteRec(tableName string, id int) error {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return err
	}

	if err = tableFile.DeleteRec(id); err != nil {
		return err
	}

	return nil
}

// GetLastID takes a table name and returns the greatest record
// id found in the table.
func (store *Store) GetLastID(tableName string) (int, error) {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return 0, err
	}

	return tableFile.GetLastID(), nil
}

// IDs takes a table name and returns an array of all record IDs
// found in the table.
func (store *Store) IDs(tableName string) ([]int, error) {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return nil, err
	}

	return tableFile.IDs(), nil
}

// InsertRec takes a table name, a record id, and a byte array and adds
// the record to the table.
func (store *Store) InsertRec(tableName string, id int, rec []byte) error {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return err
	}

	ids := tableFile.IDs()
	for _, i := range ids {
		if id == i {
			return dberr.ErrIDExists
		}
	}

	offset, err := tableFile.OffsetForWritingRec(len(rec))
	if err != nil {
		return err
	}

	if err := tableFile.WriteRec(offset, 0, rec); err != nil {
		return err
	}

	tableFile.Offsets[id] = offset

	return nil
}

// ReadRec takes a table name and an id, reads the record from the
// table, and returns a populated byte array.
func (store *Store) ReadRec(tableName string, id int) ([]byte, error) {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return nil, err
	}

	rec, err := tableFile.ReadRec(id)
	if err != nil {
		return nil, err
	}

	return rec, err
}

// RemoveTable takes a table name and deletes that table file from the
// disk.
func (store *Store) RemoveTable(tableName string) error {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return err
	}

	tableFile.Close()

	delete(store.Tables, tableName)

	return nil
}

// TableExists takes a table name and returns a bool indicating
// whether or not the table exists in the datastore.
func (store *Store) TableExists(tableName string) bool {
	_, ok := store.Tables[tableName]

	return ok
}

// Close closes the datastore.
func (store *Store) Close() error {
	for _, tableFile := range store.Tables {
		if err := tableFile.Close(); err != nil {
			return err
		}
	}

	store.Tables = nil

	return nil
}

// TableNames returns an array of table names.
func (store *Store) TableNames() []string {
	var names []string

	for k := range store.Tables {
		names = append(names, k)
	}

	return names
}

// UpdateRec takes a table name, a record id, and a byte array and updates
// the table record with that id.
func (store *Store) UpdateRec(tableName string, id int, rec []byte) error {
	tableFile, err := store.GetTableFile(tableName)
	if err != nil {
		return err
	}

	if err = tableFile.UpdateRec(id, rec); err != nil {
		return err
	}

	return nil
}

func (store *Store) GetTableFile(tableName string) (*Table, error) {
	tableFile, ok := store.Tables[tableName]
	if !ok {
		return nil, dberr.ErrNoTable
	}

	return tableFile, nil
}

func (store *Store) CloseTable(tableName string) error {
	tableFile, ok := store.Tables[tableName]
	if !ok {
		return dberr.ErrNoTable
	}

	if err := tableFile.Close(); err != nil {
		return err
	}

	return nil
}
