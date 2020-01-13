package cluster

import (
	"context"
	"fmt"
	"net/rpc"
)

type Client struct {
	*rpc.Client
	registry Registry
}

func NewClient(serviceName string, r Registry) (*Client, error) {
	serv, err := r.Services(context.Background())
	if err != nil {
		return nil, err
	}

	nodes, ok := serv[serviceName]
	if !ok || len(nodes) == 0 {
		return nil, fmt.Errorf("failed to find service %v in the registry", serviceName)
	}

	// naively pick first element; algorithm for choosing node can be improved
	dailAddr := fmt.Sprintf("%v:%v", nodes[0].Address, nodes[0].Port)
	client, err := rpc.DialHTTP("tcp", dailAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial service address %v: %w", dailAddr, err)
	}

	return &Client{
		Client:   client,
		registry: r,
	}, nil
}
