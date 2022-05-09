package paxos

import (
	"fmt"
	"github.com/AllenShaw19/paxoskv/log"
	pb "github.com/AllenShaw19/paxoskv/proto"
	"github.com/golang/protobuf/proto"
)

type CmdIds uint8

const (
	kCmdPaxos        CmdIds = 1
	kCmdRangeCatchup        = 2
	kCmdReplay              = 11
	kCmdRead                = 12
	kCmdWrite               = 13
	kCmdRecover             = 14
	kCmdCatchup             = 15
	kCmdLoad                = 16
	kCmdDumpEntry           = 21
)

type CmdBase struct {
	CmdId  uint8
	Uuid   uint64
	Result int32

	EntityId uint64
	Entry    uint64

	TimestampUs uint64
}

func NewCmdBase(cmdId uint8) *CmdBase {
	return &CmdBase{CmdId: cmdId}
}

func (b *CmdBase) setFromHeader(header *pb.CmdHeader) {
	b.Uuid = header.Uuid
	b.EntityId = header.EntityId
	b.Entry = header.Entry
	b.Result = header.Result
}

func (b *CmdBase) toHeader() *pb.CmdHeader {
	header := &pb.CmdHeader{}
	header.Uuid = b.Uuid
	header.EntityId = b.EntityId
	header.Entry = b.Entry
	header.Result = b.Result
	return header
}

type MsgBase struct {
	*CmdBase
	LocalAcceptorId uint32
	PeerAcceptorId  uint32
}

func NewMsgBase(cmdId uint8) *MsgBase {
	base := NewCmdBase(cmdId)
	return &MsgBase{CmdBase: base}
}

type PaxosCmd struct {
	*MsgBase
	// For plog worker.
	PlogLoad      bool
	PlogRangeLoad bool
	PlogGetRecord bool
	PlogSetRecord bool
	PlogReturnMsg bool
	CheckEmpty    bool
	CatchUp       bool
	StoredValueId uint64

	LocalEntryRecord *pb.EntryRecord
	PeerEntryRecord  *pb.EntryRecord

	MaxCommittedRecord uint64
	MaxChosenEntry     uint64
}

func NewPaxosCmd() *PaxosCmd {
	base := NewMsgBase(uint8(kCmdPaxos))
	cmd := &PaxosCmd{MsgBase: base}
	return cmd
}
func NewPaxosCmdWithEntry(entityId, entry uint64) *PaxosCmd {
	base := NewMsgBase(uint8(kCmdPaxos))
	base.EntityId = entityId
	base.Entry = entry
	cmd := &PaxosCmd{
		MsgBase: base,
	}
	return cmd
}

func (c *PaxosCmd) String() string {
	local := EntryRecordToString(c.LocalEntryRecord)
	peer := EntryRecordToString(c.PeerEntryRecord)

	buffer := fmt.Sprintf("%d uuid %d E(%d, %d) max %d aid(%d, %d) ce %d ca %d l(%s) p(%s) ret %d",
		c.CmdId,
		c.Uuid,
		c.EntityId,
		c.Entry,
		c.MaxChosenEntry,
		c.LocalAcceptorId,
		c.PeerAcceptorId,
		c.CheckEmpty,
		c.CatchUp,
		local,
		peer,
		c.Result)
	return buffer
}

func (c *PaxosCmd) ParseFromBuffer(buffer []byte) error {
	msg := &pb.PaxosMsg{}
	err := proto.Unmarshal(buffer, msg)
	if err != nil {
		log.Error("ParseFromArray fail", log.Err(err))
		return err
	}
	c.setFromHeader(msg.Header)

	c.LocalAcceptorId = msg.LocalAcceptorId
	c.PeerAcceptorId = msg.PeerAcceptorId

	c.LocalEntryRecord = msg.LocalEntryRecord
	c.PeerEntryRecord = msg.PeerEntryRecord

	c.MaxChosenEntry = msg.MaxChosenEntry
	c.CheckEmpty = msg.CheckEmpty
	c.CatchUp = msg.Catchup
	return nil
}

func (c *PaxosCmd) SerializeToBuffer() ([]byte, error) {
	msg := &pb.PaxosMsg{}

	msg.Header = c.toHeader()

	msg.LocalAcceptorId = c.LocalAcceptorId
	msg.PeerEntryRecord = c.PeerEntryRecord

	msg.LocalEntryRecord = c.LocalEntryRecord
	msg.PeerEntryRecord = c.PeerEntryRecord

	msg.Catchup = c.CatchUp
	msg.CheckEmpty = c.CheckEmpty
	msg.MaxChosenEntry = c.MaxChosenEntry

	buffer, err := proto.Marshal(msg)
	if err != nil {
		log.Error("SerializeToBuffer fail", log.Err(err))
		return nil, err
	}
	return buffer, nil
}

func (c *PaxosCmd) SerializedByteSize() (uint32, error) {
	buffer, err := c.SerializeToBuffer()
	if err != nil {
		return 0, err
	}
	return uint32(len(buffer)), nil
}

func (c *PaxosCmd) SwitchToLocalView(localAcceptorId uint32) bool {
	if c.PeerAcceptorId != localAcceptorId {
		return false
	}
	c.LocalAcceptorId, c.PeerAcceptorId = c.PeerAcceptorId, c.LocalAcceptorId
	c.LocalEntryRecord, c.PeerEntryRecord = c.PeerEntryRecord, c.LocalEntryRecord
	return true
}

func (c *PaxosCmd) RemoveValueInRecord() {
	// Remove in local view.
	if c.PeerEntryRecord.ValueId == 0 {
		return
	}
	if c.LocalEntryRecord.ValueId == c.PeerEntryRecord.ValueId &&
		!c.LocalEntryRecord.HasValueIdOnly {
		c.LocalEntryRecord.Value = nil
		c.LocalEntryRecord.Uuids = nil
		c.LocalEntryRecord.HasValueIdOnly = true
	}
	if !c.PeerEntryRecord.HasValueIdOnly {
		c.PeerEntryRecord.Value = nil
		c.PeerEntryRecord.Uuids = nil
		c.PeerEntryRecord.HasValueIdOnly = true
	}
}

func (c *PaxosCmd) SetChosen(value bool) {
	c.LocalEntryRecord.Chosen = value
}

type ClientCmd struct {
	*CmdBase
	ValueId   uint64
	TimeoutMs uint64
	Value     string
	Uuids     []uint64
}

func NewClientCmd(cmdId uint8) *ClientCmd {
	base := NewCmdBase(uint8(kCmdPaxos))
	cmd := &ClientCmd{
		CmdBase: base,
	}
	return cmd
}

func (c *ClientCmd) String() string {
	buffer := fmt.Sprintf("%d uuid %d E(%d, %d) vid %d v.sz %d uuids.sz %d ret %d",
		c.CmdId,
		c.Uuid,
		c.EntityId,
		c.Entry,
		c.ValueId,
		len(c.Value),
		len(c.Uuids),
		c.Result)
	return buffer
}
