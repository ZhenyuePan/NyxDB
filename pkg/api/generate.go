package api

//go:generate protoc -I ../../api --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ../../api/nyxdb.proto
