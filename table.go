package hare

type Table struct {
	db   *Database `json:"-"`
	Name string    `json:"name"`
	ID   int       `json:"_id"`
}

// Find takes a record id, and a pointer to a struct that
// implements the Record interface, finds the associated record from the
// table, and populates the struct.
func (tbl *Table) Find(id int, rec Record) error {
	return tbl.db.Find(tbl.Name, id, rec)
}

// IDs returns a list of all record ids for that table.
func (tbl *Table) IDs() ([]int, error) {
	return tbl.db.IDs(tbl.Name)
}

// Insert takes a struct that implements the Record
// interface and adds a new record to the table.  It returns the
// new record's id.
func (tbl *Table) Insert(rec Record) (int, error) {
	return tbl.db.Insert(tbl.Name, rec)
}

// Update takes a struct that implements the Record
// interface and updates the record in the table that has that record's
// id.
func (tbl *Table) Update(rec Record) error {
	return tbl.db.Update(tbl.Name, rec)
}

// Delete takes a record id and removes that
// record from the database.
func (tbl *Table) Delete(id int) error {
	return tbl.db.Delete(tbl.Name, id)
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
