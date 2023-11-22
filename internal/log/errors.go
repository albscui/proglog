package log

import "fmt"

type OffsetOutOfRangeError struct {
	Offset uint64
}

func (err *OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("offset out of range: %d", err.Offset)
}
