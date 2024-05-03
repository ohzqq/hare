package net

import (
	"net/url"
	"path/filepath"
	"strings"

	"github.com/ohzqq/hare/datastores/store"
)

type Net struct {
	*store.Store
	*url.URL
}

func New(uri string, data []byte) (*Net, error) {
	n := &Net{
		Store: store.New(),
	}

	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	n.URL = u

	tableName := filepath.Base(n.Path)
	tableName = strings.TrimSuffix(tableName, filepath.Ext(n.Path))

	err = n.Store.CreateTable(tableName, store.NewMemFile(data))
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Net) CreateTable(tableName string) error {
	return n.Store.CreateTable(tableName, store.NewMemFile([]byte{}))
}
