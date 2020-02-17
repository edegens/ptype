package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"net/rpc"
	"time"
)

func init() {
	// Initalize the seed used for the RPC client.
	rand.Seed(time.Now().UnixNano())
}

type Client struct {
	*rpc.Client
	registry Registry
}

func NewClient(serviceName string, r Registry) (*Client, error) {
	node, err := nodeToDial(serviceName, r)
	if err != nil {
		return nil, err
	}

	dialAddr := fmt.Sprintf("%v:%v", node.Address, node.Port)
	client, err := rpc.DialHTTP("tcp", dialAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial service address %v: %w", dialAddr, err)
	}

	return &Client{
		Client:   client,
		registry: r,
	}, nil
}

func nodeToDial(serviceName string, r Registry) (node Node, err error) {
	serv, err := r.Services(context.Background())
	if err != nil {
		return node, err
	}

	nodes, ok := serv[serviceName]
	if !ok || len(nodes) == 0 {
		return node, fmt.Errorf("failed to find service %v in the registry", serviceName)
	}

	randIndex := rand.Intn(len(nodes))
	return nodes[randIndex], nil
}
