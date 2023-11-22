package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// OffsetOutOfRangeError is returned when the client tries to consume an offset that's outside of the log.
type OffsetOutOfRangeError struct {
	Offset uint64
}

func (e OffsetOutOfRangeError) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("offset out of range: %d", e.Offset))
	msg := fmt.Sprintf("The request offset is outside the log's range: %d", e.Offset)
	d := &errdetails.LocalizedMessage{Locale: "en-US", Message: msg}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

func (e *OffsetOutOfRangeError) Error() string {
	return e.GRPCStatus().Err().Error()
}
