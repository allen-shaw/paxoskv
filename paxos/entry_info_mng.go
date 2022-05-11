package paxos

import (
	"github.com/AllenShaw19/paxoskv/utils"
)

type EntryKey struct {
	EntityId uint64
	Entry    uint64
}

func (k *EntryKey) Less(other *EntryKey) bool {
	if k.EntityId != other.EntityId {
		return k.EntityId < other.EntityId
	}
	return k.Entry < other.Entry
}

func (k *EntryKey) Equal(other *EntryKey) bool {
	return k.EntityId == other.EntityId && k.Entry == other.Entry
}

func (k *EntryKey) Hash() uint64 {
	return utils.HashUint64()
}

type EntryInfo struct {
	EntityInfo
}
