package main

import (
	"fmt"

	"github.com/etcd-io/etcd/raft"
)

func main() {
	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}})

	// TODO figure out how to prove that the nodes are able to communicate
	for {
		rd := n.Ready()
		fmt.Printf("%#v\n", rd.Messages)
	}
}
