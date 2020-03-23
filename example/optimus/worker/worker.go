package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"

	"github.com/edegens/ptype/cluster"
	prime "github.com/edegens/ptype/example/optimus"
)

func main() {
	prime := new(prime.Prime)
	if err := rpc.Register(prime); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	cfg, err := cluster.ConfigFromFile(os.Getenv("CONFIG"))
	if err != nil {
		log.Fatal(err)
	}

	c, err := cluster.Join(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	services, err := c.Registry.Services(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("server: services %v\n", services)

	if err := http.ListenAndServe(fmt.Sprintf(":%v", cfg.Port), nil); err != nil {
		log.Fatal(err)
	}
}
