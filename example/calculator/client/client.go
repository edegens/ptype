package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/edegens/ptype/cluster"
	"github.com/edegens/ptype/example/calculator"
)

func main() {
	cfg, err := cluster.ConfigFromFile(os.Getenv("CONFIG"))
	if err != nil {
		log.Fatal(err)
	}
	capnslog.SetGlobalLogLevel(capnslog.ERROR)

	c, err := cluster.Join(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	services, err := c.Registry.Services(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client: services %v\n", services)

	client, err := c.NewClient("calculator")
	if err != nil {
		log.Fatal(err)
	}

	args := &calculator.Args{A: 7, B: 8}
	var reply int
	err = client.Call("Calculator.Multiply", args, &reply)
	if err != nil {
		log.Fatal("client error:", err)
	}
	fmt.Printf("client: %d*%d=%d\n", args.A, args.B, reply)
}
