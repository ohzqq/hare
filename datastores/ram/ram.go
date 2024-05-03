package ram

import (
	"github.com/dsnet/golib/memfile"
	"github.com/ohzqq/hare/datastores/store"
)

const dummyRune = 'X'

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
	path string
	ext  string
	*store.Store
}

func NewRam(tables map[string][]byte) (*Ram, error) {
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

// padTooShortError is a place to hold a custom error used
// as part of a switch.
type padTooShortError struct {
}

func (e padTooShortError) Error() string {
	return "all padded records are too short"
}

func PadRec(padLength int) []byte {
	extraData := make([]byte, padLength)

	extraData[0] = '\n'

	for i := 1; i < padLength; i++ {
		extraData[i] = dummyRune
	}

	return extraData
}
