package ram

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/ohzqq/hare/dberr"
)

func TestNewCloseRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//New...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantOffsets := make(map[int]int64)
			wantOffsets[1] = 0
			wantOffsets[2] = 101
			wantOffsets[3] = 160
			wantOffsets[4] = 224

			gotOffsets := dsk.Tables["contacts"].Offsets

			if !reflect.DeepEqual(wantOffsets, gotOffsets) {
				t.Errorf("want %v; got %v", wantOffsets, gotOffsets)
			}
		},
		func(t *testing.T) {
			//Close...

			dsk := newTestRam(t)
			dsk.Close()

			wantErr := dberr.ErrNoTable
			_, gotErr := dsk.ReadRec("contacts", 3)

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}

			got := dsk.Tables

			if nil != got {
				t.Errorf("want %v; got %v", nil, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestCreateTableRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//CreateTable...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.CreateTable("newtable", Mem([]byte{}))
			if err != nil {
				t.Fatal(err)
			}

			want := true
			got := dsk.TableExists("newtable")

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//CreateTable (TableExists error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrTableExists
			gotErr := dsk.CreateTable("contacts", Mem([]byte{}))

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestDeleteRecRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//DeleteRec...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.DeleteRec("contacts", 3)
			if err != nil {
				t.Fatal(err)
			}

			want := dberr.ErrNoRecord
			_, got := dsk.ReadRec("contacts", 3)

			if !errors.Is(got, want) {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//DeleteRec (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			gotErr := dsk.DeleteRec("nonexistent", 3)

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestGetLastIDRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//GetLastID...

			dsk := newTestRam(t)
			defer dsk.Close()

			want := 4
			got, err := dsk.GetLastID("contacts")
			if err != nil {
				t.Fatal(err)
			}

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//GetLastID (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			_, gotErr := dsk.GetLastID("nonexistent")

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestIDsRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//IDs...

			dsk := newTestRam(t)
			defer dsk.Close()

			want := []int{1, 2, 3, 4}
			got, err := dsk.IDs("contacts")
			if err != nil {
				t.Fatal(err)
			}

			sort.Ints(got)

			if len(want) != len(got) {
				t.Errorf("want %v; got %v", want, got)
			} else {

				for i := range want {
					if want[i] != got[i] {
						t.Errorf("want %v; got %v", want, got)
					}
				}
			}
		},
		func(t *testing.T) {
			//IDs (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			_, gotErr := dsk.IDs("nonexistent")

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestInsertRecRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//InsertRec...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.InsertRec("contacts", 5, []byte(`{"id":5,"first_name":"Rex","last_name":"Stout","age":77}`))
			if err != nil {
				t.Fatal(err)
			}

			rec, err := dsk.ReadRec("contacts", 5)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"id\":5,\"first_name\":\"Rex\",\"last_name\":\"Stout\",\"age\":77}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//InsertRec (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			gotErr := dsk.InsertRec("nonexistent", 5, []byte(`{"id":5,"first_name":"Rex","last_name":"Stout","age":77}`))

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
		func(t *testing.T) {
			//InsertRec (IDExists error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrIDExists
			gotErr := dsk.InsertRec("contacts", 3, []byte(`{"id":3,"first_name":"Rex","last_name":"Stout","age":77}`))
			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}

			rec, err := dsk.ReadRec("contacts", 3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"id\":3,\"first_name\":\"Bill\",\"last_name\":\"Shakespeare\",\"age\":18}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestReadRecRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//ReadRec...

			dsk := newTestRam(t)
			defer dsk.Close()

			rec, err := dsk.ReadRec("contacts", 3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"id\":3,\"first_name\":\"Bill\",\"last_name\":\"Shakespeare\",\"age\":18}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//ReadRec (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			_, gotErr := dsk.ReadRec("nonexistent", 3)

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestRemoveTableRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//RemoveTable...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.RemoveTable("contacts")
			if err != nil {
				t.Fatal(err)
			}

			want := false
			got := dsk.TableExists("contacts")

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//RemoveTable (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			gotErr := dsk.RemoveTable("nonexistent")

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestTableExistsRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//TableExists...

			dsk := newTestRam(t)
			defer dsk.Close()

			want := true
			got := dsk.TableExists("contacts")

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}

			want = false
			got = dsk.TableExists("nonexistant")

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestTableNamesRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//TableNames...

			dsk := newTestRam(t)
			defer dsk.Close()

			want := []string{"contacts"}
			got := dsk.TableNames()

			sort.Strings(got)

			if len(want) != len(got) {
				t.Errorf("want %v; got %v", want, got)
			} else {

				for i := range want {
					if want[i] != got[i] {
						t.Errorf("want %v; got %v", want, got)
					}
				}
			}
		},
	}

	runTestFns(t, tests)
}

func TestUpdateRecRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//UpdateRec...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.UpdateRec("contacts", 3, []byte(`{"id":3,"first_name":"William","last_name":"Shakespeare","age":77}`))
			if err != nil {
				t.Fatal(err)
			}

			rec, err := dsk.ReadRec("contacts", 3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"id\":3,\"first_name\":\"William\",\"last_name\":\"Shakespeare\",\"age\":77}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//UpdateRec (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			gotErr := dsk.UpdateRec("nonexistent", 3, []byte(`{"id":3,"first_name":"William","last_name":"Shakespeare","age":77}`))

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}

func TestCloseTableRamTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//closeTable...

			dsk := newTestRam(t)
			defer dsk.Close()

			err := dsk.CloseTable("contacts")
			if err != nil {
				t.Errorf("want %v; got %v", nil, err)
			}
		},
		func(t *testing.T) {
			//closeTable (NoTable error)...

			dsk := newTestRam(t)
			defer dsk.Close()

			wantErr := dberr.ErrNoTable
			gotErr := dsk.CloseTable("nonexistent")

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}
		},
	}

	runTestFns(t, tests)
}
