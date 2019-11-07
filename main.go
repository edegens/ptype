package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/edegens/ptype/store"
)

func main() {
	var isMaster bool

	flag.BoolVar(&isMaster, "ismaster", false, "Use in-memory storage for Raft")
	flag.Parse()

	if isMaster {
		raftDir := "./master"
		os.MkdirAll(raftDir, 0700)

		log.Println("starting cluster")
		s, err := store.NewCluster(raftDir, ":12000", "node1")
		if err != nil {
			log.Fatal(err)
			return
		}
		time.Sleep(10 * time.Second)
		err = s.Join("node2", ":13000")
		if err != nil {
			log.Fatal(err)
			return
		}
	} else {
		raftDir := "./follower1"
		os.MkdirAll(raftDir, 0700)

		log.Println("Starting node2")
		_, err := store.NewNode(raftDir, ":13000", "node2")
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	for {
	}
}
