package table

import "bytes"

type memIO struct {
	r *bytes.Reader
	w *bytes.Buffer
}

func Mem(d []byte) *memIO {
	return &memIO{
		r: bytes.NewReader(d),
		w: bytes.NewBuffer(d),
	}
}

func (m *memIO) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *memIO) Seek(offset int64, whence int) (int64, error) {
	return m.r.Seek(offset, whence)
}

func (m *memIO) Write(p []byte) (int, error) {
	return m.w.Write(p)
}

func (m *memIO) Close() error { return nil }
