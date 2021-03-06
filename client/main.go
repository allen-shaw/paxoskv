package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	kv "paxoskv/paxoskv"
	"time"
)

const address = "localhost:50051"

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := kv.NewKvClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	// Prepare请求
	rsp, err := c.Prepare(ctx, &kv.Proposer{Id: &kv.PaxosInstanceId{Key: "k", Ver: 1}, Bal: &kv.BallotNum{N: 1, ProposerId: 2}, Val: &kv.Value{Vi64: 10}})
	if err != nil {
		log.Printf("call prepare err %v\n", err)
	}
	log.Printf("rsp %v\n", rsp)
}
