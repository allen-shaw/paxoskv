package paxos

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrInvalidAcceptorId = errors.New("invalid acceptor id")
	ErrInvalidRecord     = errors.New("invalid record")
	ErrInvalidEntryState = errors.New("invalid entry state")
)
