package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/albscui/proglog/api/v1"
)

// Log manages the list of segments.
type Log struct {
	mu sync.RWMutex // grant access to reads when there isn't a write holding the lock.

	Dir    string
	Config Config

	activeSegment *segment // the active segment to append to
	segments      []*segment
}

// NewLog creates a new log, and sets up the instance.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{Dir: dir, Config: c}
	return l, l.setup()
}

// When a log starts, it's responsible for setting itself up for the segments that already exist on disk or,
// if the log is new and has no existing segments, for boot-strapping the initial segments.
// Fetch the list of segments on disk, parse and sort the base offset (oldest to newest).
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	// i += 2 because we have dups for index in baseOffsets
	for i := 0; i < len(baseOffsets); i += 2 {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}
	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// Append appends a record to the log. We append the record to the active segment.
// Afterward, if the segments is at its max size, then make a new active segment.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		if err := l.newSegment(off + 1); err != nil {
			return off, err
		}
	}
	return off, nil
}

// Read reads the record stored at the given offset.
// First find the segment that contains the given record. Since the segments are in the order of oldest to newest and
// the segment's base offset is the smallest offset in the segment, we iterate over the segments until
// we find the first segment whose base offset is less than or equal to the offset we're looking for.
// Once we know the segment that contains the record, we get the index entry from the segment's index,
// and we read the data out of the segment's stored file and return the data to the caller.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// Close iterates over the segments and closes them.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes the log and then removes the data.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes the log and then creates a new log to replace it.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset returns the lowest offset. We will use this to help implement coordinated consensus.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset. We will use this to help implement coordinated consensus.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all segemnts whose highest offset is lower than lowest.
// Bcause we don't have disks with infinite space, we clean up the oldest data.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var keep []*segment
	for _, segment := range l.segments {
		if segment.nextOffset-1 <= lowest {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		keep = append(keep, segment)
	}
	l.segments = keep
	return nil
}

// Reader returns a Reader to read the whole log.
// Concatenates all segments' stores.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{store: segment.store, off: 0}
	}
	return io.MultiReader(readers...)
}

// The segment stoes are wrapped by originReader for two reasons:
// 1. To satisfy io.Reader.
// 2. Ensure that we begin reading from the origin of the store and read its entire file.
type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// Creates a new segment, appends that segment to the log's slice of segments,
// and makes the new segment the active segment so that subsequent append calls write to it.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
