package disk

import (
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/ohzqq/hare/datastores/store"
)

func runTestFns(t *testing.T, tests []func(t *testing.T)) {
	for i, fn := range tests {
		testSetup(t)
		t.Run(strconv.Itoa(i), fn)
		testTeardown(t)
	}
}

func newTestDisk(t *testing.T) *Disk {
	dsk, err := New("./testdata", ".json")
	if err != nil {
		t.Fatalf("newTestDisk error %v\n", err)
	}

	return dsk
}

func newTestTableFile(t *testing.T) *store.Table {
	filePtr, err := os.OpenFile("./testdata/contacts.json", os.O_RDWR, 0660)
	if err != nil {
		t.Fatalf("test new table open file error %v\n", err)
	}

	tf, err := store.NewTable(filePtr)
	if err != nil {
		t.Fatalf("newTestTableFile new table error %v\n", err)
	}

	return tf
}

func testSetup(t *testing.T) {
	testRemoveFiles(t)

	cmd := exec.Command("cp", "./testdata/contacts.bak", "./testdata/contacts.json")
	if err := cmd.Run(); err != nil {
		t.Fatalf("test cp error %v\n", err)
	}
}

func testTeardown(t *testing.T) {
	testRemoveFiles(t)
}

func testRemoveFiles(t *testing.T) {
	filesToRemove := []string{"contacts.json", "newtable.json"}

	for _, f := range filesToRemove {
		err := os.Remove("./testdata/" + f)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("test rm files error %v\n", err)
		}
	}
}
