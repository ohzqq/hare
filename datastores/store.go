package datastores

import (
	"github.com/ohzqq/hare/dberr"
)

type Store struct {
	tableFiles map[string]*Table
}

// DeleteRec takes a table name and a record id and deletes
// the associated record.
func (dsk *Disk) DeleteRec(tableName string, id int) error {
	tableFile, err := dsk.getTableFile(tableName)
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
func (dsk *Disk) GetLastID(tableName string) (int, error) {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return 0, err
	}

	return tableFile.GetLastID(), nil
}

// IDs takes a table name and returns an array of all record IDs
// found in the table.
func (dsk *Disk) IDs(tableName string) ([]int, error) {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return nil, err
	}

	return tableFile.IDs(), nil
}

// InsertRec takes a table name, a record id, and a byte array and adds
// the record to the table.
func (dsk *Disk) InsertRec(tableName string, id int, rec []byte) error {
	tableFile, err := dsk.getTableFile(tableName)
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
func (dsk *Disk) ReadRec(tableName string, id int) ([]byte, error) {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return nil, err
	}

	rec, err := tableFile.ReadRec(id)
	if err != nil {
		return nil, err
	}

	return rec, err
}

// TableExists takes a table name and returns a bool indicating
// whether or not the table exists in the datastore.
func (dsk *Disk) TableExists(tableName string) bool {
	_, ok := dsk.tableFiles[tableName]

	return ok
}

// TableNames returns an array of table names.
func (dsk *Disk) TableNames() []string {
	var names []string

	for k := range dsk.tableFiles {
		names = append(names, k)
	}

	return names
}

// UpdateRec takes a table name, a record id, and a byte array and updates
// the table record with that id.
func (dsk *Disk) UpdateRec(tableName string, id int, rec []byte) error {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return err
	}

	if err = tableFile.UpdateRec(id, rec); err != nil {
		return err
	}

	return nil
}

func (dsk *Disk) getTableFile(tableName string) (*Table, error) {
	tableFile, ok := dsk.tableFiles[tableName]
	if !ok {
		return nil, dberr.ErrNoTable
	}

	return tableFile, nil
}

func (dsk *Disk) closeTable(tableName string) error {
	tableFile, ok := dsk.tableFiles[tableName]
	if !ok {
		return dberr.ErrNoTable
	}

	if err := tableFile.Close(); err != nil {
		return err
	}

	return nil
}
