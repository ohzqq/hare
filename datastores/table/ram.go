package table

import (
	"github.com/dsnet/golib/memfile"
)

type memIO struct {
	*memfile.File
}

func Mem(d []byte) *memIO {
	return &memIO{
		File: memfile.New(d),
	}
}

func (m *memIO) Close() error { return nil }
