package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	kv "paxoskv/paxoskv"
)

const port = ":50051"

type server struct {
	kv.KvServer
}

func (s *server) Prepare(c context.Context, r *kv.Proposer) (*kv.Acceptor, error)  {
	fmt.Printf("Prepare %v", r)
	acc := &kv.Acceptor{
		LastBal: &kv.BallotNum{
			N: 1,
			ProposerId: 10,
		},
		Val: &kv.Value{
			Vi64: 100,
		},
		VBal: &kv.BallotNum{
			N: 2,
			ProposerId: 20,
		},
	}
	return acc, nil
}

func (s *server) Accept(c context.Context, r *kv.Proposer) (*kv.Acceptor, error)  {
	fmt.Println("Accepter")
	return nil, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listenL %v", err)
	}

	grpcServer := grpc.NewServer()
	kv.RegisterKvServer(grpcServer, &server{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
