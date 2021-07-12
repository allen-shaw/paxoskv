package paxoskv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const AcceptorBasePort = 4035

var ErrNotEnoughQuorum = errors.New("not enough quorum")

type Proposer struct {
	ID  *PaxosInstanceID
	Bal *BallotNum
	Val *Value
}

type Acceptor struct {
	LastBal *BallotNum
	Val     *Value
	VBal    *BallotNum
}

type Instance struct {
	mu       sync.Mutex
	acceptor *Acceptor
}

type Instances map[int64]*Instance

type KVServer struct {
	mu      sync.Mutex
	Storage map[string]Instances
}

func (s *KVServer) getInstance(id *PaxosInstanceID) *Instance {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.Key
	ver := id.Ver

	instances, found := s.Storage[key]
	if !found {
		instances = Instances{}
		s.Storage[key] = instances
	}

	v, found := instances[ver]
	if !found {
		instances[ver] = &Instance{
			acceptor: &Acceptor{
				LastBal: &BallotNum{},
				VBal:    &BallotNum{},
			},
		}
		v = instances[ver]
	}
	return v
}

// Acceptor

func (s *KVServer) Prepare(c context.Context, r *PrepareReq) (*PrepareResp, error) {
	instance := s.getInstance(r.ID)
	instance.mu.Lock()
	defer instance.mu.Unlock()

	resp := &PrepareResp{
		LastBal: instance.acceptor.LastBal,
		Val:     instance.acceptor.Val,
		VBal:    instance.acceptor.VBal,
	}

	if compareBalNum(r.Bal, instance.acceptor.LastBal) > 0 {
		instance.acceptor.LastBal = r.Bal
	}

	return resp, nil
}

func (s *KVServer) Accept(c context.Context, r *AcceptReq) (*AcceptResp, error) {
	instance := s.getInstance(r.ID)
	instance.mu.Lock()
	defer instance.mu.Unlock()

	resp := &AcceptResp{
		OK:      false,
		LastBal: instance.acceptor.LastBal,
	}
	if compareBalNum(r.Bal, instance.acceptor.LastBal) >= 0 {
		instance.acceptor.LastBal = r.Bal
		instance.acceptor.VBal = r.Bal
		instance.acceptor.Val = r.Val
		resp.OK = true
	}

	return resp, nil
}

// utils

func compareBalNum(a *BallotNum, b *BallotNum) int {
	if a.N == b.N {
		return int(a.ProposerId - b.ProposerId)
	}
	return int(a.N - b.N)
}

// Proposer
func (p *Proposer) Phase1(acceptorIDs []int64, quorum int) (*Value, *BallotNum, error) {
	replies, err := p.prepare(acceptorIDs)
	if err != nil {
		log.Fatalf("prepare to all fail, err: %v", err)
	}

	ok := 0
	higherBal := p.Bal
	maxVoted := &PrepareResp{VBal: &BallotNum{}}

	for _, reply := range replies {
		// TODO: 如果reply有更大的BallotNum,整个Phase1应该失败
		if compareBalNum(p.Bal, reply.LastBal) < 0 {
			higherBal = reply.LastBal
			continue
		}

		if compareBalNum(reply.VBal, maxVoted.VBal) > 0 {
			maxVoted = reply
		}

		ok += 1
		if ok >= quorum {
			return maxVoted.Val, nil, nil
		}
	}

	return nil, higherBal, ErrNotEnoughQuorum
}

func (p *Proposer) Phase2(acceptorIDs []int64, quorum int) (*BallotNum, error) {
	replise, err := p.Accept(acceptorIDs)
	if err != nil {
		log.Fatalf("accept to all fail, err: %v", err)
	}

	ok := 0
	higherBal := p.Bal
	for _, reply := range replise {
		if compareBalNum(p.Bal, reply.LastBal) < 0 {
			higherBal = reply.LastBal
			continue
		}
		ok += 1
		if ok >= quorum {
			return nil, nil
		}
	}

	// TODO: error 要换一个
	return higherBal, ErrNotEnoughQuorum
}

func (p *Proposer) prepare(acceptorIDs []int64) ([]*PrepareResp, error) {
	replies := make([]*PrepareResp, 0, len(acceptorIDs))

	clients, err := p.getAcceptorConn(acceptorIDs)
	if err != nil {
		log.Fatalf("get acceptor conn fail, err %v", err)
	}

	for _, client := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := &PrepareReq{
			ID:  p.ID,
			Bal: p.Bal,
		}
		reply, err := client.Prepare(ctx, req)
		if err != nil {
			log.Fatalf("Prepare fail, err %v", err)
			continue
		}
		replies = append(replies, reply)
	}

	return replies, err
}

func (p *Proposer) Accept(acceptorIDs []int64) ([]*AcceptResp, error) {
	replies := make([]*AcceptResp, 0, len(acceptorIDs))

	clients, err := p.getAcceptorConn(acceptorIDs)
	if err != nil {
		log.Fatalf("get acceptor conn fail, err %v", err)
	}

	for _, client := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := &AcceptReq{
			ID:  p.ID,
			Bal: p.Bal,
			Val: p.Val, // the value should from input
		}
		reply, err := client.Accept(ctx, req)
		if err != nil {
			log.Fatalf("Prepare fail, err %v", err)
			continue
		}
		replies = append(replies, reply)
	}

	return replies, err
}

func (p *Proposer) getAcceptorConn(acceptorIDs []int64) ([]PaxosKVClient, error) {

	clients := make([]PaxosKVClient, 0, len(acceptorIDs))

	for _, aid := range acceptorIDs {
		// service discover
		address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+aid)
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			// TODO: use other logs
			log.Fatalf("connect to acceptor %v fail, err %v", aid, err)
		}
		defer conn.Close()

		// TODO:不必每次都newclient，而是建立一个conn pool
		c := NewPaxosKVClient(conn)
		clients = append(clients, c)
	}

	return clients, nil
}

func (p *Proposer) RunPaxos(acceptorIDs []int64, val *Value) *Value {
	quorum := len(acceptorIDs)/2 + 1
	for {
		p.Val = val
		maxVotedVal, higherBal, err := p.Phase1(acceptorIDs, quorum)
		if err != nil {
			p.Bal.N = higherBal.N+1
			continue
		}

		if maxVotedVal != nil {
			p.Val = maxVotedVal
		}

		// val == nil 是读操作
		// 没有读到voted值不需要Phase2
		// 说明这个值还没确定（即使有其他写入也不满足多数派）
		if p.Val == nil {
			return nil
		}

		higherBal, err = p.Phase2(acceptorIDs, quorum)
		if err != nil {
			p.Bal.N = higherBal.N + 1
			continue
		}

		return p.Val
	}
}
