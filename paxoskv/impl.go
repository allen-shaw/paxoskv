package paxoskv

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"sync"
	"time"
)

var AcceptorBasePort = int64(3333)
var NotEnoughQuorum = errors.New("not enough qourum")

type Version struct {
	mu       sync.Mutex
	acceptor Acceptor
}

type Versions map[int64]*Version

type KVServer struct {
	mu      sync.Mutex
	Storage map[string]Versions
}

func (a *BallotNum) GE(b *BallotNum) bool {
	if a.N > b.N {
		return true
	}
	if a.N < b.N {
		return false
	}
	return a.ProposerId >= b.ProposerId
}

func (s *KVServer) Prepare(c context.Context, r *Proposer) (*Acceptor, error) {
	v := s.getLockedVersion(r.Id)
	defer v.mu.Unlock()

	reply := v.acceptor

	if r.Bal.GE(v.acceptor.LastBal) {
		v.acceptor.LastBal = r.Bal
	}

	return &reply, nil
}

// 从KVServer.Storage中根据request 发来的PaxosInstanceId中的字段key和ver获取一个指定Acceptor的实例
func (s *KVServer) getLockedVersion(id *PaxosInstanceId) *Version {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.Key
	ver := id.Ver

	rec, found := s.Storage[key]
	if !found {
		rec = Versions{}
		s.Storage[key] = rec
	}

	v, found := rec[ver]
	if !found {
		rec[ver] = &Version{
			acceptor: Acceptor{
				LastBal: &BallotNum{},
				VBal:    &BallotNum{},
			},
		}
		v = rec[ver]
	}

	v.mu.Lock()
	return v
}

func (s *KVServer) Accept(c *context.Context, r *Proposer) (*Acceptor, error) {
	v := s.getLockedVersion(r.Id)
	defer v.mu.Unlock()

	reply := Acceptor{
		LastBal: &*v.acceptor.LastBal,
	}

	if r.Bal.GE(v.acceptor.LastBal) {
		v.acceptor.LastBal = r.Bal
		v.acceptor.Val = r.Val
		v.acceptor.VBal = r.Bal
	}

	return &reply, nil
}

func (p *Proposer) Phase1(acceptorIds []int64, quorum int) (*Value, *BallotNum, error) {
	replies := p.rpcToAll(acceptorIds, "Prepare")
	ok := 0
	higherBal := *p.Bal
	maxVoted := &Acceptor{VBal: &BallotNum{}}

	for _, r := range replies {
		if !p.Bal.GE(r.LastBal) {
			higherBal = *r.LastBal
			continue
		}

		if r.VBal.GE(maxVoted.VBal) {
			maxVoted = r
		}

		ok += 1
		if ok == quorum {
			return maxVoted.Val, nil, nil
		}
	}

	return nil, &higherBal, NotEnoughQuorum
}

func (p *Proposer) rpcToAll(acceptorIds []int64, action string) []*Acceptor {
	replies := []*Acceptor{}

	for _, aid := range acceptorIds {
		var err error
		address := fmt.Sprintf("127.0.0.1:%d", AcceptorBasePort+int64((aid)))

		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		c := NewKvClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var reply *Acceptor
		if action == "Prepare" {
			reply, err = c.Prepare(ctx, p)
		} else if action == "Accept" {
			reply, err = c.Accept(ctx, p)
		}

		if err != nil {
			continue
		}
		replies = append(replies, reply)
	}
	return replies
}

func (p *Proposer) Phase2(acceptorIds []int64, quorum int) (*BallotNum, error) {
	replies := p.rpcToAll(acceptorIds, "Accept")

	ok := 0
	higherBal := *p.Bal

	for _, r := range replies {
		if !p.Bal.GE(r.LastBal) {
			higherBal = *r.LastBal
			continue
		}
		ok += 1
		if ok == quorum {
			return nil, nil
		}
	}

	return &higherBal, NotEnoughQuorum
}

func (p *Proposer) RunPaxos(acceptorIds []int64, val *Value) *Value {
	quorum := len(acceptorIds)/2 + 1
	for {
		p.Val = val
		maxVotedVal, hingerBal, err := p.Phase1(acceptorIds, quorum)
		if err != nil {
			p.Bal.N = hingerBal.N + 1
			continue
		}

		if maxVotedVal != nil {
			p.Val = maxVotedVal
		}

		if p.Val == nil {
			return nil
		}

		hingerBal, err = p.Phase2(acceptorIds, quorum)
		if err != nil {
			p.Bal.N = hingerBal.N + 1
			continue
		}

		return p.Val
	}
}

func ServeAcceptors(acceptorIds []int64) []*grpc.Server {
	servers := []*grpc.Server{}

	for _, aid := range acceptorIds {
		addr := fmt.Sprintf(":%d", AcceptorBasePort+int64(aid))

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("listen: %s %v", addr, err)
		}
		s := grpc.NewServer()
		RegisterKvServer(s, &KVServer{
			Storage: map[string]Versions{},
		})
		reflection.Register(s)
		log.Printf("Acceptor-%d serving on %s ...", aid, addr)

		servers = append(servers, s)
		go s.Serve(lis)
	}

	return servers
}
