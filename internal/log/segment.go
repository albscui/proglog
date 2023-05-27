package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/albscui/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// Segment wraps the index and store types to coordinate operations across the two.
// For example, when the log appends a new record to the active segment, the segment
// needs to write the data to its store and add a new entry in the index.
// Similarly, for reads, the segment needs to look up the entry from the index and then fetch the data from the store.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// The log calls newSegment when it needs to add a new segment, such as when the current active segment hits its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// the os.O_CREATE creates the file if they don't exist yet.
	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append a new record to the segment, and returns the newly appended record's offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	offset = s.nextOffset
	record.Offset = offset
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	// index offsets are relative to baseOffset
	if err = s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)), pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	return offset, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	return record, proto.Unmarshal(p, record)
}

// IsMaxed returns whether the segment has reached its max size, either by writing too much to the store or the index.
// If you wrote a small number of long logs, then you'd hit the sotre limit.
// If you wrote a lot of small logs, then you'd hit the index limit.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multipel of k in j
// for example, nearestMultiple(9, 4) == 8
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
