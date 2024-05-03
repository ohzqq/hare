package table

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
)

func runTestFns(t *testing.T, tests []func(t *testing.T)) {
	for i, fn := range tests {
		testSetup(t)
		t.Run(strconv.Itoa(i), fn)
		testTeardown(t)
	}
}

func newTestDisk(t *testing.T) *Disk {
	dsk, err := NewDisk("./testdata", ".json")
	if err != nil {
		t.Fatalf("newTestDisk error %v\n", err)
	}

	return dsk
}

func newTestRam(t *testing.T) *Ram {
	d, err := os.ReadFile("./testdata/contacts.json")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string][]byte{
		"contacts": d,
	}

	ram, err := NewRam(tables)
	if err != nil {
		t.Fatalf("newTestRam error %v\n", err)
	}
	return ram
}

func newTestTableFile(t *testing.T) *Table {
	filePtr, err := os.OpenFile("./testdata/contacts.json", os.O_RDWR, 0660)
	if err != nil {
		t.Fatalf("test new table open file error %v\n", err)
	}

	tf, err := NewTable(filePtr)
	if err != nil {
		t.Fatalf("newTestTableFile new table error %v\n", err)
	}

	return tf
}

func newTestTableMem(t *testing.T) *Table {
	d, err := os.ReadFile("./testdata/contacts.json")
	if err != nil {
		t.Fatal(err)
	}

	mem := Mem(d)

	tf, err := NewTable(mem)
	if err != nil {
		t.Fatal(err)
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
