package table

import (
	"os"
	"path/filepath"
)

func File(path, name, ext string) (TableIO, error) {
	return OpenFile(path, name, ext)
}

func OpenFile(path, tableName, ext string) (*os.File, error) {
	p := filepath.Join(path, tableName+ext)
	filePtr, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}

	return filePtr, nil
}
