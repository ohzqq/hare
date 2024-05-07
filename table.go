package hare

import (
	"encoding/json"

	"github.com/ohzqq/hare/dberr"
)

type Table struct {
	db   *Database `json:"-"`
	Name string    `json:"name"`
	ID   int       `json:"_id"`
}

// Close closes the table.
func (tbl *Table) Close() error {
	//tbl.db.locks[tbl.Name].Unlock()
	tbl.db = nil
	return nil
}

// Find takes a record id, and a pointer to a struct that
// implements the Record interface, finds the associated record from the
// table, and populates the struct.
func (tbl *Table) Find(id int, rec Record) error {
	if !tbl.db.TableExists(tbl.Name) {
		return dberr.ErrNoTable
	}

	tbl.db.locks[tbl.Name].Lock()
	defer tbl.db.locks[tbl.Name].Unlock()

	rawRec, err := tbl.db.store.ReadRec(tbl.Name, id)
	if err != nil {
		return err
	}

	err = json.Unmarshal(rawRec, rec)
	if err != nil {
		return err
	}

	err = rec.AfterFind(tbl.db)
	if err != nil {
		return err
	}

	return nil
}

// IDs returns a list of all record ids for that table.
func (tbl *Table) IDs() ([]int, error) {
	if !tbl.db.TableExists(tbl.Name) {
		return nil, dberr.ErrNoTable
	}

	tbl.db.locks[tbl.Name].Lock()
	defer tbl.db.locks[tbl.Name].Unlock()

	ids, err := tbl.db.store.IDs(tbl.Name)
	if err != nil {
		return nil, err
	}

	return ids, err
}

// Insert takes a struct that implements the Record
// interface and adds a new record to the table.  It returns the
// new record's id.
func (tbl *Table) Insert(rec Record) (int, error) {
	if !tbl.db.TableExists(tbl.Name) {
		return 0, dberr.ErrNoTable
	}

	tbl.db.locks[tbl.Name].Lock()
	defer tbl.db.locks[tbl.Name].Unlock()

	id := tbl.db.incrementLastID(tbl.Name)
	rec.SetID(id)

	rawRec, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}

	if err := tbl.db.store.InsertRec(tbl.Name, id, rawRec); err != nil {
		return 0, err
	}

	return id, nil
}

// Update takes a struct that implements the Record
// interface and updates the record in the table that has that record's
// id.
func (tbl *Table) Update(rec Record) error {
	if !tbl.db.TableExists(tbl.Name) {
		return dberr.ErrNoTable
	}

	tbl.db.locks[tbl.Name].Lock()
	defer tbl.db.locks[tbl.Name].Unlock()

	id := rec.GetID()

	rawRec, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if err := tbl.db.store.UpdateRec(tbl.Name, id, rawRec); err != nil {
		return err
	}

	return nil
}

// Delete takes a record id and removes that
// record from the database.
func (tbl *Table) Delete(id int) error {
	if !tbl.db.TableExists(tbl.Name) {
		return dberr.ErrNoTable
	}

	tbl.db.locks[tbl.Name].Lock()
	defer tbl.db.locks[tbl.Name].Unlock()

	if err := tbl.db.store.DeleteRec(tbl.Name, id); err != nil {
		return err
	}

	return nil
}

// Satisfy Record interface so a table can also be a record.
func (tbl *Table) SetID(id int) {
	tbl.ID = id
}

func (tbl *Table) GetID() int {
	return tbl.ID
}

func (tbl *Table) AfterFind(db *Database) error {
	tbl.db = db
	return nil
}