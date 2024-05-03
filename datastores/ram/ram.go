package ram

import (
	"github.com/ohzqq/hare/datastores/store"
)

type Ram struct {
	*store.Store
}

func New(tables map[string][]byte) (*Ram, error) {
	ram := &Ram{
		Store: store.New(),
	}

	for tableName, data := range tables {
		err := ram.Store.CreateTable(tableName, store.NewMemFile(data))
		if err != nil {
			return nil, err
		}
	}
	return ram, nil
}

func (ram *Ram) CreateTable(tableName string) error {
	return ram.Store.CreateTable(tableName, store.NewMemFile([]byte{}))
}
