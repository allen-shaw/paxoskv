package utils

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
)

func HashString(data string) uint64 {
	return xxhash.Sum64String(data)
}

func HashUint64(data uint64) uint64 {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, data)
	return xxhash.Sum64(buf)
}
