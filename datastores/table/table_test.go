package table

import (
	"os"
	"path/filepath"
	"testing"
)

const hareTestDB = `testdata/hare`

func TestNewTable(t *testing.T) {
	f, err := File(hareTestDB, "index", ".json")
	if err != nil {
		t.Error(err)
	}

	table, err := NewTable(f)
	if err != nil {
		t.Error(err)
	}
	want := 7251
	if len(table.offsets) != want {
		t.Errorf("got %v, wanted %v\n", len(table.offsets), want)
	}
}

func TestMemTable(t *testing.T) {
	d, err := os.ReadFile(filepath.Join(hareTestDB, "index.json"))
	if err != nil {
		t.Error(err)
	}

	mem := Mem(d)

	table, err := NewTable(mem)
	if err != nil {
		t.Error(err)
	}
	want := 7251
	if len(table.offsets) != want {
		t.Errorf("got %v, wanted %v\n", len(table.offsets), want)
	}
}
