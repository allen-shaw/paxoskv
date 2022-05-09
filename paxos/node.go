package paxos

import (
	"github.com/AllenShaw19/paxoskv/log"
)

var (
	queueMng *AsyncQueueMng
)

func init() {

}

type Node struct {
	options *Options
	route   Route
	plog    Plog
	db      Db
	monitor Monitor
	started bool
	workers []Worker
}

func NewNode() *Node {

}

func (n *Node) Init(options *Options, route Route, plog Plog, db Db) error {
	n.route = route
	n.plog = plog
	n.db = db
	n.monitor = options.Monitor

	n.options = options
	n.options.LocalAddr = route.GetLocalAddr()

	err := n.initManagers()
	if err != nil {
		log.Error("init managers fail", log.Err(err))
		return err
	}
	err = n.initWorkers()
	if err != nil {
		log.Error("init workers fail", log.Err(err))
		return err
	}
	return nil
}

func (n *Node) Destroy() {

}

func (n *Node) Options() *Options {
	return n.options
}

func (n *Node) Route() Route {
	return n.route
}

func (n *Node) Plog() Plog {
	return n.plog
}

func (n *Node) Db() Db {
	return n.db
}

func (n *Node) Monitor() Monitor {
	return n.monitor
}

func (n *Node) Started() bool {
	return n.started
}

func (n *Node) Write(options *CmdOptions, entityId, entry uint64, value string, uuids []uint64) error {
	write := NewClientCmd(kCmdWrite)
	write.EntityId = entityId
	write.Entry = entry
	write.Value = value
	write.Uuids = uuids
	write.TimeoutMs = uint64(options.ClientCmdTimeoutMs)

	err := PushClientCmd(write)
	if err != nil {
		return err
	}
	return write.Result
}

func (n *Node) Read(options *CmdOptions, entityId, entry uint64) error {

}

func (n *Node) Replay(options *CmdOptions, entityId uint64) (entry uint64, err error) {

}

func (n *Node) EvictEntity(entityId uint64) error {

}

func (n *Node) GetWriteValue(entityId, entry uint64) (writeValue string, err error) {

}

func (n *Node) Run() {

}

func (n *Node) WaitExit() {

}

// The following methods Called in Run().
func (n *Node) startWorkers() {

}
func (n *Node) stopWorkers() {

}

// The following methods Called in Init().
func (n *Node) initManagers() error {

}
func (n *Node) initWorkers() error {

}

// The following methods Called in Destroy().
func (n *Node) destroyManagers() {

}
func (n *Node) destroyWorkers() {

}

func PushClientCmd(cmd *ClientCmd) error {
	queue := queueMng.UserReqQueueByEntityId(cmd.EntityId)
	queue <- cmd

}
