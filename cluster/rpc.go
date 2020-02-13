package cluster

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

func init() {
	// Initalize the seed used for the RPC client.
	rand.Seed(time.Now().UnixNano())
}

const DefaultMaxClientConnections = 3

type Client struct {
	currentHost string
	registry    Registry
	clients     []*rpc.Client
	lock        sync.RWMutex
}

func newClient(host string, serviceName string, r Registry) (*Client, error) {
	c := &Client{
		currentHost: host,
		registry:    r,
		clients:     []*rpc.Client{},
	}
	return c, nil
}

func (c *Client) connectToNodes(nodes []Node, maxConnections int) error {
	clients := []*rpc.Client{}

	for i := 0; len(clients) < maxConnections; i++ {
		index := c.indexForConnection(i, len(nodes))
		node := nodes[index]

		dialAddr := fmt.Sprintf("%v:%v", node.Address, node.Port)
		client, err := rpc.DialHTTP("tcp", dialAddr)
		if err != nil {
			return fmt.Errorf("failed to dial service address %v: %w", dialAddr, err)
		}
		clients = append(clients, client)
	}

	c.lock.Lock()
	c.clients = clients
	c.lock.Unlock()

	return nil
}

func (c *Client) indexForConnection(clientNumber, nodeSize int) int {
	h := fnv.New32a()
	h.Write([]byte(c.currentHost + strconv.Itoa(clientNumber)))
	return int(h.Sum32()) % nodeSize
}

func (c *Client) watchForNewNodes(serviceName string) error {
	nodesChan := c.registry.WatchService(context.Background(), serviceName)
	for nodes := range nodesChan {
		if len(nodes) == 0 {
			continue
		}
		if err := c.connectToNodes(nodes, DefaultMaxClientConnections); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) getConnection(index int) *rpc.Client {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clients[index]
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
