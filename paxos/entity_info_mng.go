package paxos

import (
	"fmt"
	"github.com/AllenShaw19/paxoskv/utils"
	"sync"
)

type EntityInfo struct {
	EntityId        uint64
	AcceptorNum     uint32
	LocalAcceptorId uint32

	MaxContChosenEntry uint64
	MaxCatchUpEntry    uint64
	MaxChosenEntry     uint64
	MaxPlogEntry       uint64

	PreAuthEntry         uint64
	ActivePeerAcceptorId uint64

	// The newest msg received for the entity
	// when the entity_info is loading.
	WaitingMsg *PaxosCmd

	// The current client cmd.
	ClientCmd *ClientCmd

	UuidBase uint64
	RefCount int32

	EntryList  []EntryInfo
	TimerEntry utils.EltEntry[EntryInfo]

	RecoverTimestampMs uint64

	Loading        bool
	RangeLoading   bool
	RecoverPending bool
}

func (e *EntityInfo) String() string {
	s := fmt.Sprintf("e %d an %d local %d entrys: %d %d %d %d auth %d active %d "+
		"msg %v cli %v ref %d load %v range %v recover %v",
		e.EntityId,
		e.AcceptorNum,
		e.LocalAcceptorId,
		e.MaxContChosenEntry,
		e.MaxCatchUpEntry,
		e.MaxChosenEntry,
		e.MaxPlogEntry,
		e.PreAuthEntry,
		e.ActivePeerAcceptorId,
		e.WaitingMsg != nil,
		e.ClientCmd != nil,
		e.RefCount,
		e.Loading,
		e.RangeLoading,
		e.RecoverPending,
	)
	return s
}

type EntityInfoMng struct {
	options *Options
	monitor Monitor

	// entity_id -> entity_info
	entityInfos map[uint64]*EntityInfo
}

func NewEntityInfoMng(options *Options) *EntityInfoMng {
	return &EntityInfoMng{
		options: options,
		monitor: options.Monitor,
	}
}

func (m *EntityInfoMng) FindEntityInfo(entityId uint64) *EntityInfo {
	if e, ok := m.entityInfos[entityId]; ok {
		return e
	}
	return nil
}

func (m *EntityInfoMng) CreateEntityInfo(entityId uint64, acceptorNum, localAcceptorId uint32) *EntityInfo {

}

func (m *EntityInfoMng) DestroyEntityInfo(entityInfo *EntityInfo) {

}
func (m *EntityInfoMng) NextEntityInfo() *EntityInfo {

}
func (m *EntityInfoMng) MakeEnoughRoom() bool {
	return true
}

// EntityInfoGroup
type Shard struct {
	table map[uint64]EntityInfo
	lock  sync.RWMutex
}

type EntityInfoGroup struct {
	shards []Shard
}

func (g *EntityInfoGroup) RegisterEntityInfo(entityId uint64, info *EntityInfo) {

}

func (g *EntityInfoGroup) RemoveEntityInfo(entityId uint64) {

}

func (g *EntityInfoGroup) GetMaxChosenEntry(entityId uint64) (maxChosenEntry, maxContChosenEntry uint64, err error) {

}
