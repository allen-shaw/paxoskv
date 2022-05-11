package paxos

import (
	"fmt"
	"github.com/AllenShaw19/paxoskv/log"
	pb "github.com/AllenShaw19/paxoskv/proto"
	"go.uber.org/zap"
	"unsafe"
)

type EntryState int

const (
	kNormal          EntryState = 0
	kPromiseLocal               = 1
	kPromiseRemote              = 2
	kMajorityPromise            = 3
	kAcceptRemote               = 4
	kAcceptLocal                = 5
	kChosen                     = 6
)
const kMaxAcceptorNum = 5

type EntryStateMachine struct {
	entityId        uint64
	entry           uint64
	acceptorNum     uint32
	localAcceptorId uint32

	// TODO: use array to reduce gc
	entryRecords []*pb.EntryRecord
	emptyFlags   []bool

	entryState EntryState
}

func NewEntryStateMachine(entityId, entry uint64, acceptorNum, localAcceptorId uint32) *EntryStateMachine {
	esm := &EntryStateMachine{}
	esm.entityId = entityId
	esm.entry = entry
	esm.acceptorNum = acceptorNum
	esm.localAcceptorId = localAcceptorId
	esm.entryState = kNormal
	return esm
}

func (esm *EntryStateMachine) CalcEntryState() {
	esm.entryState = kNormal
	record := esm.entryRecords[esm.localAcceptorId]

	if record.Chosen {
		esm.entryState = kChosen
		return
	}

	if record.AcceptedNum > 0 {
		var acceptedCount uint32
		for i := uint32(0); i < esm.acceptorNum; i++ {
			if esm.entryRecords[i].AcceptedNum == record.AcceptedNum {
				acceptedCount++
			}
		}
		if acceptedCount >= esm.acceptorNum/2+1 {
			esm.entryState = kChosen
			esm.entryRecords[esm.localAcceptorId].Chosen = true
			return
		}
	}

	if record.PromisedNum > record.PreparedNum {
		esm.entryState = kPromiseRemote
		if record.AcceptedNum == record.PromisedNum {
			esm.entryState = kAcceptRemote
		} else {
			assert(record.AcceptedNum < record.PromisedNum, "record.AcceptedNum < record.PromisedNum")
		}
		return
	}

	assert(record.PromisedNum == record.PreparedNum, "record.PromisedNum == record.PreparedNum")
	assert(record.AcceptedNum <= record.PromisedNum, "record.AcceptedNum <= record.PromisedNum")

	if record.PromisedNum == 0 {
		esm.entryState = kNormal
		return
	}

	esm.entryState = kPromiseLocal
	if record.AcceptedNum == record.PromisedNum {
		esm.entryState = kAcceptLocal
		return
	}

	var promisedCount uint32
	for i := uint32(0); i < esm.acceptorNum; i++ {
		if esm.entryRecords[i].PromisedNum == record.PromisedNum {
			promisedCount++
		}
	}
	if promisedCount >= esm.acceptorNum/2+1 {
		esm.entryState = kMajorityPromise
		return
	}
}

func (esm *EntryStateMachine) EntryState() EntryState {
	return esm.entryState
}

func (esm *EntryStateMachine) GetByValueId(valueId uint64) (value string, uuids []uint64, err error) {
	for i := uint32(0); i < esm.acceptorNum; i++ {
		if esm.entryRecords[i].ValueId == valueId {
			value = string(esm.entryRecords[i].Value)
			copy(uuids, esm.entryRecords[i].Uuids)
			return value, uuids, nil
		}
	}
	return "", nil, ErrNotFound
}

func (esm *EntryStateMachine) RestoreValueInRecord(record *pb.EntryRecord) error {
	assert(record.HasValueIdOnly, "record.HasValueIdOnly")
	record.HasValueIdOnly = false

	value, uuids, err := esm.GetByValueId(record.ValueId)
	if err != nil {
		log.Error("entry not found", log.Uint64("entityId", esm.entityId),
			log.Uint64("entry", esm.entry),
			log.String("st", esm.String()),
			log.Uint64("vid", record.ValueId))
		record.Chosen = false
		record.Value = nil
		record.Uuids = nil
		return err
	}
	record.Value = []byte(value)
	record.Uuids = uuids
	return nil
}

func (esm *EntryStateMachine) GetChosenValue() string {
	record := esm.entryRecords[esm.localAcceptorId]
	assert(record.Chosen, "record chosen")
	return string(record.Value)
}

func (esm *EntryStateMachine) AddChosenUuids() {
	record := esm.entryRecords[esm.localAcceptorId]
	assert(record.Chosen, "record chosen")
	for _, uuid := range record.Uuids {
		GetUuidMng().Add(esm.entityId, uuid)
	}
}

func (esm *EntryStateMachine) Update(peerAcceptorId uint32, peerRecord *pb.EntryRecord) error {
	if peerAcceptorId >= esm.acceptorNum {
		log.Error("invalid peer_acceptor_id", zap.Uint64("entityId", esm.entityId),
			zap.Uint64("entry", esm.entry),
			zap.Uint32("peer_acceptor_id", peerAcceptorId),
			zap.String("record", EntryRecordToString(peerRecord)))
		return ErrInvalidAcceptorId
	}

	//  Change it to normal record if has_value_id_only.
	if peerRecord.HasValueIdOnly || IsValidRecord(esm.entityId, esm.entry, peerRecord) {
		log.Error("invalid has_value_id_only", zap.Uint64("entityId", esm.entityId),
			zap.Uint64("entry", esm.entry),
			zap.Bool("has_value_id_only", peerRecord.HasValueIdOnly),
			zap.String("record", EntryRecordToString(peerRecord)))
		return ErrInvalidRecord
	}

	if !IsRecordNewer(esm.entryRecords[peerAcceptorId], peerRecord) {
		log.Info("in peer_record not newer than local",
			zap.String("local record", EntryRecordToString(esm.entryRecords[peerAcceptorId])),
			zap.String("in peer record", EntryRecordToString(peerRecord)))
		return nil
	}

	esm.entryRecords[peerAcceptorId] = peerRecord

	// Self-update after getting info from plog.
	if peerAcceptorId == esm.localAcceptorId {
		esm.CalcEntryState()
		return nil
	}

	if esm.entryState == kChosen {
		return nil
	}

	record := esm.entryRecords[esm.localAcceptorId]

	// Update promised_num.
	if record.PromisedNum < peerRecord.PromisedNum {
		record.PromisedNum = peerRecord.PromisedNum
	}

	// Update the proposal accepted by the chosen one or
	// the one with higher proposal number that not less than promised.
	if peerRecord.Chosen ||
		(record.PromisedNum <= peerRecord.AcceptedNum &&
			record.AcceptedNum < peerRecord.AcceptedNum) {
		record.AcceptedNum = peerRecord.AcceptedNum
		record.Chosen = peerRecord.Chosen

		if record.ValueId != peerRecord.ValueId {
			record.ValueId = peerRecord.ValueId
			record.Value = peerRecord.Value
			copy(record.Uuids, peerRecord.Uuids)
		}
	}

	esm.CalcEntryState()
	return nil
}

func (esm *EntryStateMachine) Promise(preAuth bool) error {
	record := esm.entryRecords[esm.localAcceptorId]
	pn := record.PromisedNum
	n := esm.acceptorNum

	pn = (pn+n-1)/n*n + esm.localAcceptorId + 1
	if !preAuth && pn <= n {
		// proposal number not large then n is use for pre-auth only.
		pn += n
	}

	record.PreparedNum = pn
	record.PromisedNum = pn
	esm.CalcEntryState()

	if esm.entryState != kPromiseLocal {
		return ErrInvalidEntryState
	}

	return nil
}

func (esm *EntryStateMachine) Accept(value string, valueId uint64, uuids []uint64) (bool, error) {
	if esm.entryState != kMajorityPromise &&
		!(esm.entryState == kPromiseLocal && esm.GetLocalPromisedNum() <= esm.acceptorNum) {
		return false, ErrInvalidEntryState
	}

	record := esm.entryRecords[esm.localAcceptorId]
	promisedNum := record.PromisedNum
	assert(promisedNum > 0, "promisedNum > 0")

	// Select the value with max accepted_num.
	selected := uint32(0)
	maxAcceptorNum := esm.entryRecords[0].AcceptedNum
	for i := uint32(1); i < esm.acceptorNum; i++ {
		if maxAcceptorNum < esm.entryRecords[i].AcceptedNum {
			selected = i
			maxAcceptorNum = esm.entryRecords[i].AcceptedNum
		}
	}

	var preparedValueAccepted bool
	if maxAcceptorNum > 0 {
		selectedRecord := esm.entryRecords[selected]
		record.AcceptedNum = promisedNum
		record.Value = selectedRecord.Value
		record.ValueId = selectedRecord.ValueId
		copy(record.Uuids, selectedRecord.Uuids)
		preparedValueAccepted = false
	} else {
		record.AcceptedNum = promisedNum
		record.Value = []byte(value)
		record.ValueId = valueId
		copy(record.Uuids, uuids)
		preparedValueAccepted = true
	}

	esm.CalcEntryState()
	assert(esm.entryState == kAcceptLocal, "esm.entryState == kAcceptLocal")
	return preparedValueAccepted, nil
}

func (esm *EntryStateMachine) GetLocalPromisedNum() uint32 {
	return esm.entryRecords[esm.localAcceptorId].PromisedNum
}

func (esm *EntryStateMachine) GetLocalAcceptedNum() uint32 {
	return esm.entryRecords[esm.localAcceptorId].AcceptedNum
}

func (esm *EntryStateMachine) IsLocalAcceptable() bool {
	if esm.entryState != kMajorityPromise &&
		!(esm.entryState == kPromiseLocal && esm.GetLocalPromisedNum() <= esm.acceptorNum) {
		return false
	}
	return true
}

// For read routine

func (esm *EntryStateMachine) IsLocalEmpty() bool {
	return esm.entryState == kNormal
}

func (esm *EntryStateMachine) ResetEmptyFlags() {
	assert(esm.entryState == kNormal, "esm.entryState == kNormal")
	for i := uint32(0); i < esm.acceptorNum; i++ {
		esm.emptyFlags[i] = false
	}
	esm.emptyFlags[esm.localAcceptorId] = true
}

func (esm *EntryStateMachine) SetEmptyFlag(peerAcceptorId uint32) {
	esm.emptyFlags[peerAcceptorId] = true
}

func (esm *EntryStateMachine) IsMajorityEmpty() bool {
	var count uint32
	for i := uint32(0); i < esm.acceptorNum; i++ {
		if esm.emptyFlags[i] {
			count++
		}
	}
	return count > esm.acceptorNum/2
}

func (esm *EntryStateMachine) CalcSize() uint64 {
	size := uint64(unsafe.Sizeof(*esm))
	for i := uint32(0); i < esm.acceptorNum; i++ {
		size += ByteSize(esm.entryRecords[i])
	}
	return size
}

func (esm *EntryStateMachine) String() string {
	s := fmt.Sprintf("st %d local %d r0:[%s] r1:[%s] r2:[%s]",
		esm.entryState,
		esm.localAcceptorId,
		EntryRecordToString(esm.entryRecords[0]),
		EntryRecordToString(esm.entryRecords[1]),
		EntryRecordToString(esm.entryRecords[2]))
	return s
}

func (esm *EntryStateMachine) GetEntryRecord(acceptorId uint32) *pb.EntryRecord {
	return esm.entryRecords[acceptorId]
}

func (esm *EntryStateMachine) HasPromisedMyProposal(peerAcceptorId uint32) bool {
	if esm.entryRecords[esm.localAcceptorId].PromisedNum !=
		esm.entryRecords[peerAcceptorId].PromisedNum {
		return false
	}
	if esm.entryRecords[esm.localAcceptorId].PromisedNum !=
		esm.entryRecords[esm.localAcceptorId].PreparedNum {
		return false
	}
	return true
}

func (esm *EntryStateMachine) HasAcceptedMyProposal(peerAcceptorId uint32) bool {
	if esm.entryRecords[esm.localAcceptorId].AcceptedNum !=
		esm.entryRecords[peerAcceptorId].AcceptedNum {
		return false
	}
	if esm.entryRecords[esm.localAcceptorId].AcceptedNum !=
		esm.entryRecords[esm.localAcceptorId].PreparedNum {
		return false
	}
	return true
}

func IsValidRecord(entityId, entry uint64, record *pb.EntryRecord) bool {
	if record.PreparedNum > record.PromisedNum ||
		record.PromisedNum > record.AcceptedNum {
		log.Error("invalid record", zap.Uint64("entityId", entityId),
			zap.Uint64("entry", entry),
			zap.String("record", EntryRecordToString(record)))
		return false
	}

	// A valid proposal requires: value_id > 0.
	if (record.AcceptedNum == 0 && record.ValueId > 0) ||
		(record.AcceptedNum > 0 && record.ValueId == 0) {
		log.Error("invalid record", zap.Uint64("entityId", entityId),
			zap.Uint64("entry", entry),
			zap.String("record", EntryRecordToString(record)))
		return false
	}

	if record.ValueId == 0 {
		if len(record.Value) > 0 || len(record.Uuids) > 0 || record.Chosen {
			log.Error("invalid record", zap.Uint64("entityId", entityId),
				zap.Uint64("entry", entry),
				zap.String("record", EntryRecordToString(record)))
			return false
		}
	}

	if record.HasValueIdOnly {
		if len(record.Value) > 0 || len(record.Uuids) > 0 {
			log.Error("invalid record", zap.Uint64("entityId", entityId),
				zap.Uint64("entry", entry),
				zap.String("record", EntryRecordToString(record)))
			return false
		}
	}
	return true
}

func IsRecordNewer(oldRecord, newRecord *pb.EntryRecord) bool {
	if oldRecord.Chosen {
		return false
	}
	if newRecord.Chosen {
		return true
	}

	if newRecord.PreparedNum > oldRecord.PreparedNum ||
		newRecord.PromisedNum > oldRecord.PromisedNum ||
		newRecord.AcceptedNum > oldRecord.AcceptedNum {
		return true
	}

	return false
}
