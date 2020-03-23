package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/edegens/ptype/cluster"
	prime "github.com/edegens/ptype/example/optimus"
)

var worker *cluster.Client

func main() {
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
	fmt.Printf("coordinator: services %v\n", services)

	// let the worker spin up after etcd
	time.Sleep(500 * time.Millisecond)
	worker, err = c.NewClient("prime_worker", nil)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/test", test)
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func test(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "invalid_http_method")
		return
	}
	err := r.ParseForm()
	if err != nil {
		log.Fatal(err)
	}

	target, _ := strconv.Atoi(r.Form.Get("target"))

	replyChan := make(chan int)

	splitWork(target, replyChan)
	reply := watchReplies(target, replyChan)

	fmt.Fprint(w, strconv.Itoa(reply))
}

func splitWork(target int, replyChan chan int) {
	min := 2
	for i := 10; i < target+10; i += 10 {
		go checkRange(min, i, target, replyChan)
		min = i
	}
}

func checkRange(min, max, target int, replyChan chan int) {
	args := &prime.Args{
		Min:    min,
		Max:    max,
		Target: target,
	}
	var reply int

	err := worker.Call("Prime.Check", args, &reply)
	if err != nil {
		log.Fatal("coordinator error:", err)
	}

	replyChan <- reply
}

func watchReplies(target int, replyChan chan int) int {
	for i := 10; i < target+10; i += 10 {
		reply := <-replyChan
		if reply != target {
			return reply
		}
	}
	return target
}
