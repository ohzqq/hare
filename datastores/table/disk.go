package table

import (
	"bytes"
	"os"
	"path/filepath"
)

func Mem(d []byte) (memIO, error) {
	return &memIO{
		r: bytes.NewReader(d),
		w: bytes.NewBuffer(d),
	}
}

func File(path, name, ext string) (TableIO, error) {
	return OpenFile(path, name, ext)
}

func OpenFile(path, tableName, ext string) (*os.File, error) {
	var osFlag int

	if createIfNeeded {
		osFlag = os.O_CREATE | os.O_RDWR
	} else {
		osFlag = os.O_RDWR
	}

	p := filepath.Join(path, tableName+ext)
	filePtr, err := os.OpenFile(p, osFlag, 0660)
	if err != nil {
		return nil, err
	}

	return filePtr, nil
}
