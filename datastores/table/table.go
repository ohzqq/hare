package table

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

const dummyRune = 'X'

type Table struct {
	buf     *bytes.Reader
	w       io.Writer
	offsets map[int]int64
	name    string
}

type TableIO interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

func NewTable(rw TableIO) (*Table, error) {
	d, err := io.ReadAll(rw)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewReader(d)

	offsets, err := CalculateOffsets(buf)
	if err != nil {
		return nil, err
	}

	tableFile := Table{
		buf:     buf,
		w:       rw,
		offsets: offsets,
	}

	return &tableFile, nil
}

func CalculateOffsets(s io.ReadSeeker) (map[int]int64, error) {

	offsets := make(map[int]int64)
	var totalOffset int64
	var recLen int
	var recMap map[string]interface{}
	var currentOffset int64

	r := bufio.NewReader(s)

	_, err := s.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	for {
		rec, err := r.ReadBytes('\n')

		recLen = len(rec)
		totalOffset += int64(recLen)
		currentOffset = totalOffset

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		// Skip dummy records.
		if (rec[0] == '\n') || (rec[0] == dummyRune) {
			continue
		}

		//Unmarshal so we can grab the record ID.
		if err := json.Unmarshal(rec, &recMap); err != nil {
			return nil, err
		}
		recMapID := int(recMap["id"].(float64))

		//println(string(rec))
		offsets[recMapID] = currentOffset
	}

	return offsets, nil
}
