# Distributed Log System

Based on the book [Distributed Services with Go by Travis Jeffery](https://github.com/travisjeffery/proglog).

**Progress**

- [x] Let's Go
- [x] Structured Data With Protocol Buffers
- [x] Write a Log Package
- [x] Serve Requests with gRPC
- [x] Secure Your Services
- [x] Observe Your System
- [x] Server-to-server Service Discovery
- [ ] Coordinate Your Services with Consensus
- [ ] Discover Servers and Load Balance from the Client
- [ ] Deploy with Kubernetes Locally
- [ ] Deploy with Kubernetes to the Cloud

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

## Observe

### Metrics

There are three kinds of metrics:

*Counters*

Counters track the number of times an event happened, such as the number for requests that failed or 
the sum of some fact of your system like the number of bytes processed. Counters are often used to 
calculate a rate, the number of times an event happened in an interval.

*Histograms*

Histograms show you a distribution of your data. You'll mainly use histograms for measuring the percentiles of your request duration and sizes.

*Gauges*

Gauges track the current value of something. You can replace that value entirely. Gauges are useful for saturation-type metrics, like a host's disk usage percentage or the number of load balancers compared to your cloud provider's limits.


What to measure? Here are Google's 4 golden sigals:

1. Latency - the time it takes your service to process requests.
1. Traffic - the amount of demand on your service.
1. Errors - failure rate. 
1. Saturation - a measure of your service's capacity.


### Structured Logs

A structured log is a set of name and value ordered plairs encoded in consistent schema and format that's easily read by programs.

### Traces

Traces capture request lifecycles and let you track requests as they flow through your system. Tools like Jaegar, Strackdriver, and Lightstep help with this.

You can tag your traces with details to know more about each request. A common example is tagging each trace with a user ID so that if users experience a problem, you can easily find their requests.

## Service Discovery

Two service discovery problems to solve:

- How will the servers in our cluster discover each other?
- How will the clients discover the servers?

### Embed Service Discovery

Sevice discovery tools need to perform the following tasks:

- manage a registry of services containing info such as their IPs and ports;
- help services find other services using the registry
- health check service instances and remove them if they're not well
- deregister services when they go offline

For our service, we'll use [Serf](https://github.com/hashicorp/serf).

To implement service discovery with Serf we need to:

1. Create a Serf node on each server.
1. Configure each Serf node with an address to listen on and accept connections from other Serf nodes.
1. Configure each Serf node with addresses of other Serf nodes and join their cluster.
1. Handle Serf's cluster discovery events, such as when a node joins or fails in the cluster.


## Replication

Discovery alone isn't useful. Discovery events trigger other processes in our service like replication and consensus. When servers discover other servers, we want to trigger the servers to replicate. We need a component in our service that handles when a server joins (or leaves) the cluster and begins (or ends) replicating from it.

Our replication will be pull-based, with the replication component consuming from each discovered server and producing a copy to the local server. In pull-based replication, the consumer periodically polls the data source to check if it has new data to consume. In push-based replication, the ata source pushes the data to its replicas.

We will build a component that acts as a membership handler handling When a server joins the cluster. When a server joins the cluster, the component will connect to the server and run a loop that consumes from the discovered server and produces to the local server.

## Raft

Raft breaks consensus into two parts: leader election and log replication.

### Leader Election

A Raft cluster has one leader and the rest are followers. The leader maintains power by sending heartbeat requests to its followers, effectively saying: "Remember me? I'm sitll your boss". If the follower times out waiting for a heartbeat request from the leader, then the follower becomes a candidate and begins an election to decide the new leader. The candidate votes for itself and then requests votes from the followers -- "The boss is gone! I'm the new boss now... right?" If the candidate receives a majority of the votes, it becomes the leader, and it sends heartbeat requests to the followers to establish authority: "Hey y'all, new boss here."

Followers can become candidates simultaneously if they time out at the same time waiting for the leader's heartbeats. They'll hold their own elections and the elections might not result in a new leader because of vote splitting. So they'll hold another election. Candidates will hold elections until there's a winner that becomes the new leader.

Every Raft server has a term: a monotonically increasing integer that tells other servers how authoritative and current this server is. The server's terms act as a logical clock: a way to capture chronological and causal relationships in distributed systems, where real-time clocks are untrustworthy and unimportant. Each time a candidate begins an election, it increases its term. If the candidate wins the election and becomes the leader, the followers update their terms to match and the terms don't change until the next election. Servers vote once per term for the first candidate that requests votes, as long as the candidate's term is greater than the voters'. These conditions help prevent vote splits and ensure the voters elect an up-to-date leader.

### Log Replication

The leader accepts client requests, each of which represents some command to run across the cluster. (In a key-value service for example, you'd have a command to assign a key's value). For each request, the leader appends the command to its log and then requests its followers to append the command to their logs. After a majority of followers have replicated the command -- when the leader considers the command committed -- the leader executes the command with a finite-state machine and responds to the client with the result. The leader tracks the highest committed offset and sends this in the requests to the followers. When a follower receives a request, it executes all commands up to the highest committed offset with its finite-state machine. All Raft servers run the same FSM that defines how to handle each command.

The recommended number of servers is 3 and five. The general formula is `N = 2f + 1`, where `N` is the number of servers, and `f` is the number of failures to tolerate.
