# Distributed Log System

## Core Components

Record - the data stored in our log.

Store - the file we store records in.

Index - the file we store index entries in.

Segment - the abstraction that ties a store and an index together.

Log - the abstraction that ties all the segments together.


![Alt text](image.png)

## Dev Env

Install `protoc`.

```
apt install -y protobuf-compiler
```

Install Go plugins for protocol compiler.

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

Install `cfssl`.

```
go install github.com/cloudflare/cfssl/cmd/...@latest
```
