package paxos

type Worker interface {
	Run() error
}
