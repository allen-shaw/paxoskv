package paxos

import "github.com/AllenShaw19/paxoskv/utils"

type AsyncQueue chan *CmdBase

type AsyncQueueMng struct {
	userReqQueues []AsyncQueue
	userRspQueues []AsyncQueue

	msgReqQueues       []AsyncQueue
	entityReqQueues    []AsyncQueue
	dbReqQueues        []AsyncQueue
	dbLimitedReqQueues []AsyncQueue
	catchUpReqQueues   []AsyncQueue
	recoverReqQueues   []AsyncQueue
	recoverRspQueues   []AsyncQueue
	toolsReqQueues     []AsyncQueue

	plogReqQueues         []AsyncQueue
	plogReadOnlyReqQueues []AsyncQueue
	plogRspQueues         []AsyncQueue
}

func (m *AsyncQueueMng) Init(options *Options) error {
	// msg workers.
	m.msgReqQueues = createAsyncQueues(options.MsgWorkerNum, options.MsgQueueSize)

	// entity workers.
	m.userReqQueues = createAsyncQueues(options.EntityWorkerNum, options.UserQueueSize)
	m.userRspQueues = createAsyncQueues(options.EntityWorkerNum, options.UserQueueSize)
	m.entityReqQueues = createAsyncQueues(options.EntityWorkerNum, options.EntityQueueSize)
	m.plogRspQueues = createAsyncQueues(options.EntityWorkerNum, options.PlogQueueSize)
	m.recoverRspQueues = createAsyncQueues(options.EntityWorkerNum, options.RecoverQueueSize)

	// plog workers
	m.plogReqQueues = createAsyncQueues(options.PlogWorkerNum, options.PlogQueueSize)
	m.plogReadOnlyReqQueues = createAsyncQueues(options.PlogReadonlyWorkerNum, options.PlogQueueSize)

	// db workers
	m.dbReqQueues = createAsyncQueues(options.DbWorkerNum, options.DbQueueSize)
	m.dbLimitedReqQueues = createAsyncQueues(options.DbLimitedWorkerNum, options.DbQueueSize)

	// recover workers
	m.recoverReqQueues = createAsyncQueues(options.RecoverWorkerNum, options.RecoverQueueSize)

	// catchup workers
	m.catchUpReqQueues = createAsyncQueues(options.CatchUpWorkerNum, options.CatchUpQueueSize)

	// tools workers
	m.toolsReqQueues = createAsyncQueues(options.ToolsWorkerNum, options.ToolsQueueSize)
	return nil
}

func (m *AsyncQueueMng) Destroy() {
	m.userReqQueues = nil
	m.plogReqQueues = nil
	m.toolsReqQueues = nil
	m.recoverRspQueues = nil
	m.recoverReqQueues = nil
	m.catchUpReqQueues = nil
	m.dbLimitedReqQueues = nil
	m.dbReqQueues = nil
	m.entityReqQueues = nil
	m.msgReqQueues = nil
	m.userRspQueues = nil
	m.plogReadOnlyReqQueues = nil
	m.plogRspQueues = nil
}

func (m *AsyncQueueMng) LogQueueStat(tag string, queues []AsyncQueue, num uint32) {

}

func (m *AsyncQueueMng) LogAllQueueStat() {

}

func (m *AsyncQueueMng) UserReqQueueByIdx(id uint32) AsyncQueue {
	return m.userReqQueues[id]
}

func (m *AsyncQueueMng) UserReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.userReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.userReqQueues[id]
}

func (m *AsyncQueueMng) UserRspQueueByIdx(id uint32) AsyncQueue {
	return m.userRspQueues[id]
}

func (m *AsyncQueueMng) UserRspQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.userRspQueues))
	id := utils.HashUint64(entityId) % length
	return m.userRspQueues[id]
}

func (m *AsyncQueueMng) MsgReqQueueByIdx(id uint32) AsyncQueue {
	return m.msgReqQueues[id]
}

func (m *AsyncQueueMng) MsgReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.msgReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.msgReqQueues[id]
}

func (m *AsyncQueueMng) EntityReqQueueByIdx(id uint32) AsyncQueue {
	return m.entityReqQueues[id]
}

func (m *AsyncQueueMng) EntityReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.entityReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.entityReqQueues[id]
}

func (m *AsyncQueueMng) DbReqQueueByIdx(id uint32) AsyncQueue {
	return m.dbReqQueues[id]
}

func (m *AsyncQueueMng) DbReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.dbReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.dbReqQueues[id]
}

func (m *AsyncQueueMng) DbLimitedReqQueueByIdx(id uint32) AsyncQueue {
	return m.dbLimitedReqQueues[id]
}
func (m *AsyncQueueMng) DbLimitedReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.dbLimitedReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.dbLimitedReqQueues[id]
}

func (m *AsyncQueueMng) CatchUpReqQueueByIdx(id uint32) AsyncQueue {
	return m.catchUpReqQueues[id]
}
func (m *AsyncQueueMng) CatchUpReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.catchUpReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.catchUpReqQueues[id]
}

func (m *AsyncQueueMng) RecoverReqQueueByIdx(id uint32) AsyncQueue {
	return m.recoverReqQueues[id]
}
func (m *AsyncQueueMng) RecoverReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.recoverReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.recoverReqQueues[id]
}

func (m *AsyncQueueMng) RecoverRspQueueByIdx(id uint32) AsyncQueue {
	return m.recoverRspQueues[id]
}
func (m *AsyncQueueMng) RecoverRspQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.recoverRspQueues))
	id := utils.HashUint64(entityId) % length
	return m.recoverRspQueues[id]
}

func (m *AsyncQueueMng) ToolsReqQueueByIdx(id uint32) AsyncQueue {
	return m.toolsReqQueues[id]
}
func (m *AsyncQueueMng) ToolsReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.toolsReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.toolsReqQueues[id]
}

func (m *AsyncQueueMng) PlogReqQueueByIdx(id uint32) AsyncQueue {
	return m.plogReqQueues[id]
}
func (m *AsyncQueueMng) PlogReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.plogReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.plogReqQueues[id]
}

func (m *AsyncQueueMng) PlogReadOnlyReqQueueByIdx(id uint32) AsyncQueue {
	return m.plogReadOnlyReqQueues[id]
}
func (m *AsyncQueueMng) PlogReadOnlyReqQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.plogReadOnlyReqQueues))
	id := utils.HashUint64(entityId) % length
	return m.plogReadOnlyReqQueues[id]
}

func (m *AsyncQueueMng) PlogRspQueueByIdx(id uint32) AsyncQueue {
	return m.plogRspQueues[id]
}
func (m *AsyncQueueMng) PlogRspQueueByEntityId(entityId uint64) AsyncQueue {
	length := uint64(len(m.plogRspQueues))
	id := utils.HashUint64(entityId) % length
	return m.plogRspQueues[id]
}

func createAsyncQueues(cnt uint32, size uint32) []AsyncQueue {
	queues := make([]AsyncQueue, 0, cnt)
	for i := uint32(0); i < cnt; i++ {
		queues[i] = make(AsyncQueue, size)
	}
	return queues
}
