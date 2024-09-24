package log

import (
	"fmt"

	api "github.com/albscui/proglog/api/v1"
)

type OffsetOutOfRangeError api.OffsetOutOfRangeError

func (err *OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("offset out of range: %d", err.Offset)
}
