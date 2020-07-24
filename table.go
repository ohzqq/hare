package hare

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
)

const dummyRune = 'X'

type Record interface {
	SetID(int)
	GetID() int
	AfterFind()
}

type Table struct {
	filePtr *os.File
	sync.RWMutex
	lastID int
	index  map[int]int64
}

func openTable(filePath string, includeCreatePerm bool) (*Table, error) {
	var err error

	tbl := new(Table)
	perm := os.O_RDWR

	if includeCreatePerm {
		perm = os.O_CREATE | os.O_RDWR
	}

	tbl.filePtr, err = os.OpenFile(filePath, perm, 0660)
	if err != nil {
		return nil, err
	}

	tbl.initIndex()
	tbl.initLastID()

	return tbl, nil
}

func (tbl *Table) IDs() []int {
	keys := make([]int, len(tbl.index))

	i := 0
	for k := range tbl.index {
		keys[i] = k
		i++
	}

	return keys
}

func (tbl *Table) Find(id int, rec Record) error {
	tbl.RLock()
	defer tbl.RUnlock()

	offset, ok := tbl.index[id]
	if !ok {
		return errors.New("Find Error: Record with ID of " + strconv.Itoa(id) + " does not exist!")
	}

	rawRec, err := tbl.readRec(offset)
	if err != nil {
		return err
	}

	err = json.Unmarshal(rawRec, rec)
	if err != nil {
		return err
	}

	rec.AfterFind()

	return nil
}

func (tbl *Table) Create(rec Record) (int, error) {
	tbl.Lock()
	defer tbl.Unlock()

	var err error
	var offset int64
	var whence int

	recID := tbl.incrementLastID()
	rec.SetID(recID)

	rawRec, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}

	// First check to see if we can fit it onto a line with a dummy record...
	offset, err = tbl.offsetToFitRec(len(rawRec))

	switch err := err.(type) {
	case nil:
		whence = 0
	case DummiesTooShortError:
		whence = 2
	default:
		return 0, err
	}

	// Line too big to fit on any dummy record line, so go to the end of file so we can add it to end of the file.
	if whence == 2 {
		offset, err = tbl.filePtr.Seek(0, 2)

		if err != nil {
			return 0, err
		}
	}

	if err = tbl.writeRec(offset, 0, rawRec); err != nil {
		return 0, err
	}

	if err != nil {
		return 0, err
	}

	tbl.index[recID] = offset

	return recID, nil
}

// Destroy takes a record ID (int) and removes the
// corresponding record from the table's json file.
func (tbl *Table) Destroy(recID int) error {
	var err error

	tbl.Lock()
	defer tbl.Unlock()

	offset, ok := tbl.index[recID]
	if !ok {
		return errors.New("Destroy Error: Record with ID of " + strconv.Itoa(recID) + " does not exist!")
	}

	rawRec, err := tbl.readRec(offset)
	if err != nil {
		return err
	}

	if err = tbl.overwriteRec(offset, len(rawRec)); err != nil {
		return err
	}

	delete(tbl.index, recID)

	return nil
}

// Update takes a Record and updates the corresponding line in the json file
// with it's contents.
func (tbl *Table) Update(rec Record) error {
	tbl.Lock()
	defer tbl.Unlock()

	var offset int64
	var goToEoF bool

	recID := rec.GetID()

	oldRecOffset, ok := tbl.index[recID]
	if !ok {
		return errors.New("Update Error: Record with ID of " + strconv.Itoa(recID) + " does not exist!")
	}

	oldRec, err := tbl.readRec(oldRecOffset)
	if err != nil {
		return err
	}

	oldRecLen := len(oldRec)

	newRec, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	newRecLen := len(newRec)

	diff := oldRecLen - (newRecLen + 1)

	if diff > 0 {
		// Changed record is smaller than record in table.

		extraData := make([]byte, diff)

		for i := range extraData {
			if i == 0 {
				extraData[i] = '\n'
			} else {
				extraData[i] = 'X'
			}
		}

		newRec = append(newRec, extraData...)

		err = tbl.writeRec(oldRecOffset, 0, newRec)
		if err != nil {
			return err
		}

	} else if diff < 0 {
		// Changed record is larger than the record in table.

		// First check to see if we can fit it onto a line with a dummy record...
		offset, err = tbl.offsetToFitRec(len(newRec))

		switch err := err.(type) {
		case nil:
		case DummiesTooShortError:
			goToEoF = true
		default:
			return err
		}

		// If we can't fit the updated record onto a line with a dummy record, then go to the End of File.
		if goToEoF {
			offset, err = tbl.filePtr.Seek(0, 2)
			if err != nil {
				return err
			}
		}

		err = tbl.writeRec(offset, 0, newRec)
		if err != nil {
			return err
		}

		// Turn old rec into a dummy.
		if err = tbl.overwriteRec(oldRecOffset, oldRecLen); err != nil {
			return err
		}

		// Update index with new offset since record is in new position in the file.
		tbl.index[recID] = offset
	} else {
		// Changed record is same length as record in table.

		err = tbl.writeRec(tbl.index[recID], 0, newRec)
		if err != nil {
			return err
		}
	}

	return nil
}

//******************************************************************************
// PRIVATE METHODS
//******************************************************************************

func (tbl *Table) incrementLastID() int {
	tbl.lastID++

	return tbl.lastID
}

func (tbl *Table) initIndex() error {
	var recOffset int64
	var totalOffset int64
	var recLength int
	var recMap map[string]interface{}

	tbl.index = make(map[int]int64)

	r := bufio.NewReader(tbl.filePtr)

	_, err := tbl.filePtr.Seek(0, 0)
	if err != nil {
		return err
	}

	for {
		rawRec, err := r.ReadBytes('\n')

		recOffset = totalOffset
		recLength = len(rawRec)
		totalOffset += int64(recLength)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		// If this is a record that has been deleted or is the result of
		// an update that left extra data on the line, then skip this
		// dummy record.
		if (rawRec[0] == '\n') || (rawRec[0] == dummyRune) {
			continue
		}

		if err := json.Unmarshal(rawRec, &recMap); err != nil {
			return err
		}

		recMapID := int(recMap["id"].(float64))

		tbl.index[recMapID] = recOffset
	}

	return nil
}

func (tbl *Table) initLastID() {
	tbl.lastID = 0

	for k := range tbl.index {
		if k > tbl.lastID {
			tbl.lastID = k
		}
	}
}

func (tbl *Table) offsetToFitRec(recLengthNeeded int) (int64, error) {
	var err error
	var recLength int
	var recOffset int64
	var totalOffset int64

	r := bufio.NewReader(tbl.filePtr)

	if _, err = tbl.filePtr.Seek(0, 0); err != nil {
		return 0, err
	}

	for {
		rawRec, err := r.ReadBytes('\n')

		recOffset = totalOffset
		recLength = len(rawRec)
		totalOffset += int64(recLength)

		// Need to account for newline character.
		recLength--

		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, err
		}

		if (rawRec[0] == '\n') || (rawRec[0] == dummyRune) {
			if recLength >= recLengthNeeded {
				return recOffset, nil
			}
		}
	}

	return 0, DummiesTooShortError{}
}

func (tbl *Table) overwriteRec(recOffset int64, recLength int) error {
	var err error

	// Overwrite record with XXXXXXXX...
	oldRecData := make([]byte, recLength-1)

	for i := range oldRecData {
		oldRecData[i] = 'X'
	}

	if err = tbl.writeRec(recOffset, 0, oldRecData); err != nil {
		return err
	}

	return nil
}

func (tbl *Table) readRec(offset int64) ([]byte, error) {
	var err error

	r := bufio.NewReader(tbl.filePtr)

	if _, err = tbl.filePtr.Seek(offset, 0); err != nil {
		return nil, err
	}

	rawRec, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	return rawRec, err
}

func (tbl *Table) writeRec(offset int64, whence int, rec []byte) error {
	var err error
	var rawRec []byte

	w := bufio.NewWriter(tbl.filePtr)

	rawRec = append(rec, '\n')

	if _, err = tbl.filePtr.Seek(offset, whence); err != nil {
		panic(err)
	}

	if _, err = w.Write(rawRec); err != nil {
		panic(err)
	}

	w.Flush()

	return nil
}

// DummiesTooShortError is a place to hold a custom error used
// as part of a switch.
type DummiesTooShortError struct {
}

func (e DummiesTooShortError) Error() string {
	return ""
}
