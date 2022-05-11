package paxos

var uuidMng *UuidMng

func GetUuidMng() *UuidMng {
	return uuidMng
}

type UuidMng struct {
}

func (m *UuidMng) Add(entityId, uuid uint64) {

}

type shard struct {
}
