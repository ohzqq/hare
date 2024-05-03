package disk

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ohzqq/hare/datastores/store"
	"github.com/ohzqq/hare/dberr"
)

// Disk is a struct that holds a map of all the
// table files in a database directory.
type Disk struct {
	path string
	ext  string
	*store.Store
}

// New takes a datastorage path and an extension
// and returns a pointer to a Disk struct.
func New(path string, ext string) (*Disk, error) {
	dsk := &Disk{
		Store: store.New(),
	}

	dsk.path = path
	dsk.ext = ext

	if err := dsk.init(); err != nil {
		return nil, err
	}

	return dsk, nil
}

func OpenFile(path, tableName, ext string) (*os.File, error) {
	p := filepath.Join(path, tableName+ext)
	filePtr, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}

	return filePtr, nil
}

// Close closes the datastore.
func (dsk *Disk) Close() error {
	err := dsk.Store.Close()
	if err != nil {
		return err
	}

	dsk.path = ""
	dsk.ext = ""

	return nil
}

// CreateTable takes a table name, creates a new disk
// file, and adds it to the map of tables in the
// datastore.
func (dsk *Disk) CreateTable(tableName string) error {
	if dsk.TableExists(tableName) {
		return dberr.ErrTableExists
	}

	filePtr, err := OpenFile(dsk.path, tableName, dsk.ext)
	if err != nil {
		return err
	}

	err = dsk.Store.CreateTable(tableName, filePtr)
	if err != nil {
		return err
	}

	return nil
}

// RemoveTable takes a table name and deletes that table file from the
// disk.
func (dsk *Disk) RemoveTable(tableName string) error {
	tableFile, err := dsk.GetTableFile(tableName)
	if err != nil {
		return err
	}

	tableFile.Close()

	if err := os.Remove(dsk.getTablePath(tableName)); err != nil {
		return err
	}

	delete(dsk.Tables, tableName)

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
	tableFile, err := dsk.GetTableFile(tableName)
	if err != nil {
		return err
	}
	defer tableFile.Close()

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
		r, err := tableFile.ReadRec(id)
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
	tableNames, err := dsk.getTableNames()
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		filePtr, err := OpenFile(dsk.path, tableName, dsk.ext)
		if err != nil {
			return err
		}

		err = dsk.Store.CreateTable(tableName, filePtr)
		if err != nil {
			return err
		}
	}

	return nil
}
