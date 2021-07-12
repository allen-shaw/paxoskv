#!/usr/bin/env zsh
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/paxoskv.proto
mv paxoskv.pb.go paxoskv_grpc.pb.go ../paxoskv