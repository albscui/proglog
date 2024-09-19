package log

import "github.com/hashicorp/raft"

type Segment struct {
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	InitialOffset uint64
}

type Raft struct {
	raft.Config
	StreamLayer *StreamLayer
	Bootstrap   bool
}

type Config struct {
	Segment Segment
	Raft    Raft
}
