package table

import (
	"github.com/dsnet/golib/memfile"
	"github.com/ohzqq/hare/dberr"
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
	path       string
	ext        string
	tableFiles map[string]*Table
}

func NewRam(tables map[string][]byte) (*Ram, error) {
	ram := &Ram{
		tableFiles: make(map[string]*Table),
	}

	for tableName, data := range tables {
		tableFile, err := NewTable(Mem(data))
		if err != nil {
			return nil, err
		}
		ram.tableFiles[tableName] = tableFile
	}
	return ram, nil
}

// Close closes the datastore.
func (ram *Ram) Close() error {
	for _, tableFile := range ram.tableFiles {
		if err := tableFile.Close(); err != nil {
			return err
		}
	}

	ram.tableFiles = nil

	return nil
}

// CreateTable takes a table name, creates a new disk
// file, and adds it to the map of tables in the
// datastore.
func (ram *Ram) CreateTable(tableName string) error {
	if ram.TableExists(tableName) {
		return dberr.ErrTableExists
	}

	tableFile, err := NewTable(Mem([]byte{}))
	if err != nil {
		return err
	}
	ram.tableFiles[tableName] = tableFile

	return nil
}

// DeleteRec takes a table name and a record id and deletes
// the associated record.
func (ram *Ram) DeleteRec(tableName string, id int) error {
	tableFile, err := ram.getTableFile(tableName)
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
func (ram *Ram) GetLastID(tableName string) (int, error) {
	tableFile, err := ram.getTableFile(tableName)
	if err != nil {
		return 0, err
	}

	return tableFile.GetLastID(), nil
}

// IDs takes a table name and returns an array of all record IDs
// found in the table.
func (ram *Ram) IDs(tableName string) ([]int, error) {
	tableFile, err := ram.getTableFile(tableName)
	if err != nil {
		return nil, err
	}

	return tableFile.IDs(), nil
}

// InsertRec takes a table name, a record id, and a byte array and adds
// the record to the table.
func (ram *Ram) InsertRec(tableName string, id int, rec []byte) error {
	tableFile, err := ram.getTableFile(tableName)
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

	tableFile.offsets[id] = offset

	return nil
}

// ReadRec takes a table name and an id, reads the record from the
// table, and returns a populated byte array.
func (ram *Ram) ReadRec(tableName string, id int) ([]byte, error) {
	tableFile, err := ram.getTableFile(tableName)
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
func (ram *Ram) RemoveTable(tableName string) error {
	tableFile, err := ram.getTableFile(tableName)
	if err != nil {
		return err
	}

	tableFile.Close()

	delete(ram.tableFiles, tableName)

	return nil
}

// TableExists takes a table name and returns a bool indicating
// whether or not the table exists in the datastore.
func (ram *Ram) TableExists(tableName string) bool {
	_, ok := ram.tableFiles[tableName]

	return ok
}

// TableNames returns an array of table names.
func (ram *Ram) TableNames() []string {
	var names []string

	for k := range ram.tableFiles {
		names = append(names, k)
	}

	return names
}

// UpdateRec takes a table name, a record id, and a byte array and updates
// the table record with that id.
func (ram *Ram) UpdateRec(tableName string, id int, rec []byte) error {
	tableFile, err := ram.getTableFile(tableName)
	if err != nil {
		return err
	}

	if err = tableFile.UpdateRec(id, rec); err != nil {
		return err
	}

	return nil
}

func (ram *Ram) getTableFile(tableName string) (*Table, error) {
	tableFile, ok := ram.tableFiles[tableName]
	if !ok {
		return nil, dberr.ErrNoTable
	}

	return tableFile, nil
}

func (ram *Ram) closeTable(tableName string) error {
	tableFile, ok := ram.tableFiles[tableName]
	if !ok {
		return dberr.ErrNoTable
	}

	if err := tableFile.Close(); err != nil {
		return err
	}

	return nil
}
