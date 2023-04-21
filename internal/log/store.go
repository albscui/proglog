package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian // the encoding that we persist record sizes and index entries in
)

const (
	lenWidth = 8 // the number of bytes used to store the record's length
)

// The store struct is a simple wrapper around a file.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a store for the given file.
func newStore(f *os.File) (*store, error) {
	// Use stat to get the file's current size,
	// in case we're re-creating the store from a file that has existing data.
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{File: f, size: size, buf: bufio.NewWriter(f)}, nil
}

// Append persists the given bytes to the store. We write the length of the record so that,
// when we read the record, we know how many bytes to read. We write to the buffered writer
// instead of of directly to the file to reduce the humber of system calls and improve performance.
// If a user wrote a lot of small records, this would help a lot.
// Return the number of bytes written, and the position of the record.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush the write buffer in case we're about to read a record still in the buffer.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// Find out how many bytes we have to read to get the whole record.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// Fetch the actual record.
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads `len(p)` bytes into `p` beginning at the off `offset` in the store's file.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
