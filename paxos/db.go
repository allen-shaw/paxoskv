package paxos

type RecoverFlag uint32

const (
	Normal RecoverFlag = iota
	Recover
)

type Db interface {
	Commit(entityId, entry uint64, value string) error
	GetStatus(entityId, maxCommittedEntry uint64) (flag RecoverFlag, err error)
	SnapshotRecover(entityId uint64, startAcceptorId uint32) (maxCommittedEntry uint64, err error)
	LockEntity(entityId uint64)
	UnlockEntity(entityId uint64)
}
