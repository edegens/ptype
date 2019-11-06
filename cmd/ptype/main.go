package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/etcd/embed"
)

func main() {
	cfgPath := os.Getenv("ETCD_CONFIG")
	cfg, err := embed.ConfigFromFile(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}

	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			fmt.Printf("%#v\n", e.Peers)
		}
	}
}
