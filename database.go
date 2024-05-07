// Package hare implements a simple DBMS that stores it's data
// in newline-delimited json files.
package hare

import (
	"sync"

	"github.com/ohzqq/hare/dberr"
)

// Record interface defines the methods a struct representing
// a table record must implement.
type Record interface {
	SetID(int)
	GetID() int
	AfterFind(*Database) error
}

type Datastorage interface {
	Close() error
	CreateTable(string) error
	DeleteRec(string, int) error
	GetLastID(string) (int, error)
	IDs(string) ([]int, error)
	InsertRec(string, int, []byte) error
	ReadRec(string, int) ([]byte, error)
	RemoveTable(string) error
	TableExists(string) bool
	TableNames() []string
	UpdateRec(string, int, []byte) error
}

// Database struct is the main struct for the Hare package.
type Database struct {
	store   Datastorage
	locks   map[string]*sync.RWMutex
	lastIDs map[string]int
}

// New takes a datastorage and returns a pointer to a
// Database struct.
func New(ds Datastorage) (*Database, error) {
	db := &Database{store: ds}
	db.locks = make(map[string]*sync.RWMutex)
	db.lastIDs = make(map[string]int)

	for _, tableName := range db.store.TableNames() {
		db.locks[tableName] = &sync.RWMutex{}

		lastID, err := db.store.GetLastID(tableName)
		if err != nil {
			return nil, err
		}
		db.lastIDs[tableName] = lastID
	}

	return db, nil
}

// Close closes the associated datastore.
func (db *Database) Close() error {
	for _, lock := range db.locks {
		lock.Lock()
	}

	if err := db.store.Close(); err != nil {
		return err
	}

	for _, lock := range db.locks {
		lock.Unlock()
	}

	db.store = nil
	db.locks = nil
	db.lastIDs = nil

	return nil
}

// GetTable takes a table name and returns the associated database table.
func (db *Database) GetTable(tableName string) (*Table, error) {
	if !db.TableExists(tableName) {
		return nil, dberr.ErrNoTable
	}

	tbl := &Table{
		Name: tableName,
	}

	err := tbl.AfterFind(db)
	if err != nil {
		return nil, err
	}

	return tbl, nil
}

// CreateTable takes a table name and creates and
// initializes a new table.
func (db *Database) CreateTable(tableName string) error {
	if db.TableExists(tableName) {
		return dberr.ErrTableExists
	}

	if err := db.store.CreateTable(tableName); err != nil {
		return nil
	}

	db.locks[tableName] = &sync.RWMutex{}

	lastID, err := db.store.GetLastID(tableName)
	if err != nil {
		return err
	}
	db.lastIDs[tableName] = lastID

	return nil
}

// Delete takes a table name and record id and removes that
// record from the database.
func (db *Database) Delete(tableName string, id int) error {
	tbl, err := db.GetTable(tableName)
	if err != nil {
		return err
	}

	if err := tbl.Delete(id); err != nil {
		return err
	}

	return nil
}

// DropTable takes a table name and deletes the table.
func (db *Database) DropTable(tableName string) error {
	if !db.TableExists(tableName) {
		return dberr.ErrNoTable
	}

	db.locks[tableName].Lock()

	if err := db.store.RemoveTable(tableName); err != nil {
		db.locks[tableName].Unlock()
		return err
	}

	delete(db.lastIDs, tableName)

	db.locks[tableName].Unlock()

	delete(db.locks, tableName)

	return nil
}

// Find takes a table name, a record id, and a pointer to a struct that
// implements the Record interface, finds the associated record from the
// table, and populates the struct.
func (db *Database) Find(tableName string, id int, rec Record) error {
	tbl, err := db.GetTable(tableName)
	if err != nil {
		return err
	}

	return tbl.Find(id, rec)
}

// IDs takes a table name and returns a list of all record ids for
// that table.
func (db *Database) IDs(tableName string) ([]int, error) {
	tbl, err := db.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	return tbl.IDs()
}

// Insert takes a table name and a struct that implements the Record
// interface and adds a new record to the table.  It returns the
// new record's id.
func (db *Database) Insert(tableName string, rec Record) (int, error) {
	tbl, err := db.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	return tbl.Insert(rec)
}

// TableExists takes a table name and returns true if the table exists,
// false if it does not.
func (db *Database) TableExists(tableName string) bool {
	return db.tableExists(tableName) && db.store.TableExists(tableName)
}

// Update takes a table name and a struct that implements the Record
// interface and updates the record in the table that has that record's
// id.
func (db *Database) Update(tableName string, rec Record) error {
	tbl, err := db.GetTable(tableName)
	if err != nil {
		return err
	}
	return tbl.Update(rec)
}

// unexported methods

func (db *Database) incrementLastID(tableName string) int {
	lastID := db.lastIDs[tableName]

	lastID++

	db.lastIDs[tableName] = lastID

	return lastID
}

func (db *Database) tableExists(tableName string) bool {
	_, ok := db.locks[tableName]
	if !ok {
		return false
	}
	_, ok = db.lastIDs[tableName]
	if !ok {
		return false
	}

	return ok
}
