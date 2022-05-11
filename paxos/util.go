package paxos

import "github.com/golang/protobuf/proto"

func assert(condition bool, msg string) {
	if !condition {
		panic(msg + " is false")
	}
}

func ByteSize(msg proto.Message) uint64 {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return 0
	}
	return uint64(len(bytes))
}
