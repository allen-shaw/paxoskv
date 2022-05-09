package paxos

type Options struct {
	OssIdKey uint32

	AcceptorNum   uint32
	EnablePreAuth uint32

	MsgWorkerNum          uint32
	EntityWorkerNum       uint32
	PlogWorkerNum         uint32
	PlogReadonlyWorkerNum uint32
	PlogRoutineNum        uint32
	DbWorkerNum           uint32
	DbRoutineNum          uint32
	DbLimitedWorkerNum    uint32
	RecoverWorkerNum      uint32
	RecoverRoutineNum     uint32
	CatchUpWorkerNum      uint32
	CatchUpRoutineNum     uint32
	ToolsWorkerNum        uint32

	DbMaxKbPerSecond         uint32
	DbMaxCountPerSecond      uint32
	RecoverMaxCountPerSecond uint32
	CatchUpMaxKbPerSecond    uint32
	CatchUpMaxCountPerSecond uint32
	CatchUpMaxGetPerSecond   uint32
	CatchUpTimeoutMs         uint32

	UserQueueSize    uint32
	MsgQueueSize     uint32
	EntityQueueSize  uint32
	PlogQueueSize    uint32
	DbQueueSize      uint32
	RecoverQueueSize uint32
	CatchUpQueueSize uint32
	ToolsQueueSize   uint32

	LocalAddr string
	Monitor   Monitor
}

type CmdOptions struct {
	ClientCmdTimeoutMs uint32
}
