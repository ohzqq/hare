package ram

import (
	"github.com/dsnet/golib/memfile"
	"github.com/ohzqq/hare/datastores/store"
)

type MemFile struct {
	*memfile.File
}

func Mem(d []byte) *MemFile {
	return &MemFile{
		File: memfile.New(d),
	}
}

func (m *MemFile) Close() error { return nil }

type Ram struct {
	*store.Store
}

func New(tables map[string][]byte) (*Ram, error) {
	ram := &Ram{
		Store: store.New(),
	}

	for tableName, data := range tables {
		err := ram.Store.CreateTable(tableName, Mem(data))
		if err != nil {
			return nil, err
		}
	}
	return ram, nil
}

func (ram *Ram) CreateTable(tableName string) error {
	return ram.Store.CreateTable(tableName, Mem([]byte{}))
}
