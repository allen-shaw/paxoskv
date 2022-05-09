package paxos

type Record struct {
	EntityId uint64
	entry    uint64
	record   string
}

type Plog interface {
	LoadMaxEntry(entityId uint64) (entry uint64, err error)
	GetValue(entityId, entry, valueId uint64) (value string, err error)
	SetValue(entityId, entry, valueId uint64, value string) error
	GetRecord(entityId, entry uint64) (record string, err error)
	SetRecord(entityId, entry uint64, record string) error
	HashId(entityId uint64) uint32
	MultiSetRecords(hashId uint32, records []Record) error
	RangeGetRecord(entityId, beginEntry, endEntry uint64) (records []Pair[uint64, string], err error)
}
