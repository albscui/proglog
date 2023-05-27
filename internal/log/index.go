package log

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

var (
	// these *Width constants define the number of bytes that make up each index entry
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

// index defines an index file, which comprises a file and a memory-mapped file.
// Index entries contain two fields: the record's offset and its position in the store file.
// Store offsets as uint32, and positions as uint64, so 4 and 8 bytes respectively.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// Creates an index for the given file. Save the current size of the file so we can track
// the amount of data in the index file as we add index entries. We grow the file to the max index size
// before memory-mapping the file. Finally return the newly created index.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.Wrap(err, "stat")
	}
	idx.size = uint64(fi.Size())
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, errors.Wrap(err, "truncate")
	}
	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "map")
	}
	return idx, nil
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset; 0 is always the offset of the index's first entry,
// 1 is the second, and so on. We use relative offsets to reduce the size of the indexes.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32(i.size/entryWidth) - 1
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entryWidth
	if i.size < pos+entryWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offsetWidth])
	pos = enc.Uint64(i.mmap[pos+offsetWidth : pos+entryWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offsetWidth], off)
	enc.PutUint64(i.mmap[i.size+offsetWidth:i.size+entryWidth], pos)
	i.size += entryWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// Close makes sure the memory-mapped file syncs its data to the persisted file,
// and that the persisted file has flushed its contents to stable storage.
// Then it truncates the persited file to the amount of data that's actually in it and closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
