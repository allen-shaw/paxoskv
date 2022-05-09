#!/bin/bash
protoc -I=./ --go_out="$GOPATH/src" *.proto