package table

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ohzqq/hare/dberr"
)

// Disk is a struct that holds a map of all the
// table files in a database directory.
type Disk struct {
	path       string
	ext        string
	tableFiles map[string]*tableFile
}

func File(path, name, ext string) (TableIO, error) {
	return OpenFile(path, name, ext)
}

func OpenFile(path, tableName, ext string) (*os.File, error) {
	p := filepath.Join(path, tableName+ext)
	filePtr, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}

	return filePtr, nil
}

// New takes a datastorage path and an extension
// and returns a pointer to a Disk struct.
func New(path string, ext string) (*Disk, error) {
	var dsk Disk

	dsk.path = path
	dsk.ext = ext

	if err := dsk.init(); err != nil {
		return nil, err
	}

	return &dsk, nil
}

// Close closes the datastore.
func (dsk *Disk) Close() error {
	for _, tableFile := range dsk.tableFiles {
		if err := tableFile.close(); err != nil {
			return err
		}
	}

	dsk.path = ""
	dsk.ext = ""
	dsk.tableFiles = nil

	return nil
}

// CreateTable takes a table name, creates a new disk
// file, and adds it to the map of tables in the
// datastore.
func (dsk *Disk) CreateTable(tableName string) error {
	if dsk.TableExists(tableName) {
		return dberr.ErrTableExists
	}

	filePtr, err := File(dsk.path, tableName, dsk.ext)
	if err != nil {
		return err
	}

	tableFile, err := NewTable(filePtr)
	if err != nil {
		return err
	}

	dsk.tableFiles[tableName] = tableFile

	return nil
}

// DeleteRec takes a table name and a record id and deletes
// the associated record.
func (dsk *Disk) DeleteRec(tableName string, id int) error {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return err
	}

	if err = tableFile.deleteRec(id); err != nil {
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

	return tableFile.getLastID(), nil
}

// IDs takes a table name and returns an array of all record IDs
// found in the table.
func (dsk *Disk) IDs(tableName string) ([]int, error) {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return nil, err
	}

	return tableFile.ids(), nil
}

// InsertRec takes a table name, a record id, and a byte array and adds
// the record to the table.
func (dsk *Disk) InsertRec(tableName string, id int, rec []byte) error {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return err
	}

	ids := tableFile.ids()
	for _, i := range ids {
		if id == i {
			return dberr.ErrIDExists
		}
	}

	offset, err := tableFile.offsetForWritingRec(len(rec))
	if err != nil {
		return err
	}

	if err := tableFile.writeRec(offset, 0, rec); err != nil {
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

	rec, err := tableFile.readRec(id)
	if err != nil {
		return nil, err
	}

	return rec, err
}

// RemoveTable takes a table name and deletes that table file from the
// disk.
func (dsk *Disk) RemoveTable(tableName string) error {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return err
	}

	tableFile.close()

	if err := os.Remove(dsk.getTablePath(tableName)); err != nil {
		return err
	}

	delete(dsk.tableFiles, tableName)

	return nil
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

	if err = tableFile.updateRec(id, rec); err != nil {
		return err
	}

	return nil
}

// CompactTable takes a table name and compacts that table file on the
// disk. (Taken from the example)
func (dsk *Disk) CompactTable(tableName string) error {
	return dsk.compactFile(tableName)
}

//******************************************************************************
// UNEXPORTED METHODS
//******************************************************************************

func (dsk *Disk) compactFile(tableName string) error {
	tableFile, err := dsk.getTableFile(tableName)
	if err != nil {
		return err
	}
	defer tableFile.close()

	tablePath := dsk.getTablePath(tableName)
	backupFilepath := strings.TrimSuffix(tablePath, dsk.ext) + ".old"

	cmd := exec.Command("cp", tablePath, backupFilepath)
	if err := cmd.Run(); err != nil {
		return err
	}

	// copy all records
	recs := make(map[int][]byte)

	ids, err := dsk.IDs(tableName)
	if err != nil {
		return err
	}

	for _, id := range ids {
		r, err := tableFile.readRec(id)
		if err != nil {
			return err
		}
		recs[id] = bytes.TrimSuffix(r, []byte("\n"))
	}

	// backup table
	err = dsk.RemoveTable(tableName)
	if err != nil {
		return err
	}

	err = dsk.CreateTable(tableName)
	if err != nil {
		return err
	}

	for id, rec := range recs {
		err = dsk.InsertRec(tableName, id, rec)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dsk *Disk) getTableFile(tableName string) (*tableFile, error) {
	tableFile, ok := dsk.tableFiles[tableName]
	if !ok {
		return nil, dberr.ErrNoTable
	}

	return tableFile, nil
}

func (dsk *Disk) getTablePath(tableName string) string {
	if dsk.TableExists(tableName) {
		return filepath.Join(dsk.path, tableName+dsk.ext)
	}
	return ""
}

func (dsk *Disk) getTableNames() ([]string, error) {
	var tableNames []string

	glob := filepath.Join(dsk.path, "*"+dsk.ext)
	files, err := filepath.Glob(glob)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), dsk.ext)
		tableNames = append(tableNames, name)
	}

	return tableNames, nil
}

func (dsk *Disk) init() error {
	dsk.tableFiles = make(map[string]*tableFile)

	tableNames, err := dsk.getTableNames()
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		filePtr, err := File(dsk.path, tableName, dsk.ext)
		if err != nil {
			return err
		}

		tableFile, err := NewTable(filePtr)
		if err != nil {
			return err
		}

		dsk.tableFiles[tableName] = tableFile
	}

	return nil
}

func (dsk Disk) openFile(tableName string, createIfNeeded bool) (*os.File, error) {
	var osFlag int

	if createIfNeeded {
		osFlag = os.O_CREATE | os.O_RDWR
	} else {
		osFlag = os.O_RDWR
	}

	p := filepath.Join(dsk.path, tableName+dsk.ext)
	filePtr, err := os.OpenFile(p, osFlag, 0660)
	if err != nil {
		return nil, err
	}

	return filePtr, nil
}

func (dsk *Disk) closeTable(tableName string) error {
	tableFile, ok := dsk.tableFiles[tableName]
	if !ok {
		return dberr.ErrNoTable
	}

	if err := tableFile.close(); err != nil {
		return err
	}

	return nil
}
