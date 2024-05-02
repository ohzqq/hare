package table

import (
	"testing"
)

const hareTestDB = `testdata/hare`

func TestNewTable(t *testing.T) {
	table, err := File(hareTestDB, "index", ".json")
	if err != nil {
		t.Error(err)
	}

	_, err = NewTable(table)
	if err != nil {
		t.Error(err)
	}
	//fmt.Printf("%#v\n", table.offsets)
}
