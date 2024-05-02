package table

import (
	"os"
	"path/filepath"
	"testing"
)

const hareTestDB = `testdata/hare`

func TestNewTable(t *testing.T) {
	f, err := os.Open(filepath.Join(hareTestDB, "index.json"))
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	_, err = NewTable(f)
	if err != nil {
		t.Error(err)
	}
	//fmt.Printf("%#v\n", table.offsets)
}
