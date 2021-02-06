package paxoskv

import "sync"

type Version struct {
	mu       sync.Mutex
	acceptor Acceptor
}

type Versions map[int64]*Version

type KVServer struct {
	mu      sync.Mutex
	Storage map[string]Versions
}
