package store

import (
	"bufio"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/ohzqq/hare/dberr"
)

func TestNewCloseTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//New...

			tf := newTestTableMem(t)
			defer tf.Close()

			want := make(map[int]int64)
			want[1] = 0
			want[2] = 102
			want[3] = 162
			want[4] = 227

			got := tf.offsets

			if !reflect.DeepEqual(want, got) {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//close...

			tf := newTestTableMem(t)
			tf.Close()

			wantErr := dberr.ErrNoRecord
			_, gotErr := tf.ReadRec(3)

			if !errors.Is(gotErr, wantErr) {
				t.Errorf("want %v; got %v", wantErr, gotErr)
			}

			got := tf.offsets

			if nil != got {
				t.Errorf("want %v; got %v", nil, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestDeleteRecTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//deleteRec...

			tf := newTestTableMem(t)
			defer tf.Close()

			offset := tf.offsets[3]

			err := tf.DeleteRec(3)
			if err != nil {
				t.Fatal(err)
			}

			want := "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n"

			r := bufio.NewReader(tf)

			if _, err := tf.Seek(offset, 0); err != nil {
				t.Fatal(err)
			}

			rec, err := r.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestGetLastIDTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//getLastID...

			tf := newTestTableMem(t)
			defer tf.Close()

			want := 4
			got := tf.GetLastID()

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestIDsTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//ids...

			tf := newTestTableMem(t)
			defer tf.Close()

			want := []int{1, 2, 3, 4}
			got := tf.IDs()
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
	}

	runTestFns(t, tests)
}

func TestOffsetsTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//offsetForWritingRec...

			tf := newTestTableMem(t)
			defer tf.Close()

			tests := []struct {
				recLen int
				want   int
			}{
				{45, 288},
				{44, 57},
			}

			for _, tt := range tests {
				want := int64(tt.want)
				got, err := tf.offsetForWritingRec(tt.recLen)
				if err != nil {
					t.Fatal(err)
				}
				if want != got {
					t.Errorf("want %v; got %v", want, got)
				}
			}
		},
		func(t *testing.T) {
			//offsetToFitRec...

			tf := newTestTableMem(t)
			defer tf.Close()

			tests := []struct {
				recLen  int
				want    int
				wanterr error
			}{
				{284, 0, paddingTooShortError{}},
				{44, 57, nil},
			}

			for _, tt := range tests {
				want := int64(tt.want)
				got, goterr := tf.offsetToFitRec(tt.recLen)
				if !((want == got) && (errors.Is(goterr, tt.wanterr))) {
					t.Errorf("want %v; wanterr %v; got %v; goterr %v", want, tt.wanterr, got, goterr)
				}
			}
		},
	}

	runTestFns(t, tests)
}

func TestOverwriteRecTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//overwriteRec...

			tf := newTestTableMem(t)
			defer tf.Close()

			offset := tf.offsets[3]

			err := tf.overwriteRec(160, 64)
			if err != nil {
				t.Fatal(err)
			}

			want := "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n"

			r := bufio.NewReader(tf)

			if _, err := tf.Seek(offset, 0); err != nil {
				t.Fatal(err)
			}

			rec, err := r.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestReadRecTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//ReadRec...

			tf := newTestTableMem(t)
			defer tf.Close()

			rec, err := tf.ReadRec(3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"_id\":3,\"first_name\":\"Bill\",\"last_name\":\"Shakespeare\",\"age\":18}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestUpdateRecTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//updateRec (fits on same line)...

			tf := newTestTableMem(t)
			defer tf.Close()

			err := tf.UpdateRec(3, []byte("{\"_id\":3,\"first_name\":\"Bill\",\"last_name\":\"Shakespeare\",\"age\":92}"))
			if err != nil {
				t.Fatal(err)
			}

			wantOffset := int64(162)
			gotOffset := tf.offsets[3]

			if wantOffset != gotOffset {
				t.Errorf("want %v; got %v", wantOffset, gotOffset)
			}

			rec, err := tf.ReadRec(3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"_id\":3,\"first_name\":\"Bill\",\"last_name\":\"Shakespeare\",\"age\":92}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
		func(t *testing.T) {
			//updateRec (does not fit on same line)...

			tf := newTestTableMem(t)
			defer tf.Close()

			err := tf.UpdateRec(3, []byte("{\"_id\":3,\"first_name\":\"William\",\"last_name\":\"Shakespeare\",\"age\":18}"))
			if err != nil {
				t.Fatal(err)
			}

			wantOffset := int64(288)
			gotOffset := tf.offsets[3]

			if wantOffset != gotOffset {
				t.Errorf("want %v; got %v", wantOffset, gotOffset)
			}

			rec, err := tf.ReadRec(3)
			if err != nil {
				t.Fatal(err)
			}

			want := "{\"_id\":3,\"first_name\":\"William\",\"last_name\":\"Shakespeare\",\"age\":18}\n"
			got := string(rec)

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	runTestFns(t, tests)
}

func TestPadRecTableMemTests(t *testing.T) {
	var tests = []func(t *testing.T){
		func(t *testing.T) {
			//padRec...

			want := "\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
			got := string(padRec(50))

			if want != got {
				t.Errorf("want %v; got %v", want, got)
			}
		},
	}

	for i, fn := range tests {
		testSetup(t)
		t.Run(strconv.Itoa(i), fn)
		testTeardown(t)
	}
}
