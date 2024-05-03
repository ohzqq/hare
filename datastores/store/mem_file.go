package store

import "github.com/dsnet/golib/memfile"

type MemFile struct {
	*memfile.File
}

func NewMemFile(d []byte) *MemFile {
	return &MemFile{
		File: memfile.New(d),
	}
}

func (m *MemFile) Close() error { return nil }
