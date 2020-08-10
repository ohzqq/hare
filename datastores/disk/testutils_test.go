package disk

import (
	"os"
	"os/exec"
	"testing"
)

func newTestTableFile(t *testing.T) *tableFile {
	filePtr, err := os.OpenFile("./testdata/contacts.json", os.O_RDWR, 0660)
	if err != nil {
		t.Fatal(err)
	}

	tf, err := NewTableFile("contacts", filePtr)
	if err != nil {
		t.Fatal(err)
	}

	return tf
}

func testSetup(t *testing.T) {
	cmd := exec.Command("cp", "./testdata/contacts.bak", "./testdata/contacts.json")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
}

func testTeardown(t *testing.T) {
	if err := os.Remove("./testdata/contacts.json"); err != nil {
		t.Fatal(err)
	}
}