package paxos

type Route interface {
	GetLocalAddr() string
	GetLocalAcceptorId(entityId uint64) (acceptorId uint32, err error)
	GetServerAddrId(entityId uint64, acceptorId uint32) (addrId uint64, err error)
}
