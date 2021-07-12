#!/usr/bin/env zsh
mdkir -p bin
go build -race -v main.go -o paxoskv.svr
mv paxoskv.svr bin