package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"

	"github.com/coreos/pkg/capnslog"
	"github.com/edegens/ptype/cluster"
	"github.com/edegens/ptype/example/calculator"
)

func main() {
	calculator := new(calculator.Calculator)
	if err := rpc.Register(calculator); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

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
	fmt.Printf("server: services %v\n", services)

	if err := http.ListenAndServe(fmt.Sprintf(":%v", cfg.Port), nil); err != nil {
		log.Fatal(err)
	}
}
