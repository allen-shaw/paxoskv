package paxoskv

import (
	"fmt"
	"testing"
)

func TestSetGet(t *testing.T) {
	acceptorIds := []int64{0, 1, 2}

	servers := ServeAcceptors(acceptorIds)
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()

	// set foo(0) = 5
	{
		prop := Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 0,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, &Value{Vi64: 5})
		fmt.Printf("written:%v;\n", v.Vi64)
	}

	// get foo(0)
	{
		prop:= Proposer{
			Id: &PaxosInstanceId{
				Key: "foo",
				Ver: 0,
			},
			Bal: &BallotNum{N: 0, ProposerId: 2},
		}
		v := prop.RunPaxos(acceptorIds, nil)
		fmt.Printf("read: %v;\n", v.Vi64)
	}
}
