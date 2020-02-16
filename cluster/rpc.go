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

	"go.uber.org/zap"
)

func init() {
	// Initalize the seed used for the RPC client.
	rand.Seed(time.Now().UnixNano())
}

const DefaultMaxClientConnections = 3

type Client struct {
	clientNode string
	registry   Registry

	nodesConnected map[Node]struct{}
	clients        []*rpc.Client
	lock           sync.RWMutex

	// Watcher
	cancel    func()
	waitGroup sync.WaitGroup
}

func newClient(host string, serviceName string, r Registry) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		clientNode: host,
		registry:   r,
		clients:    []*rpc.Client{},
		cancel:     cancel,
	}
	c.waitGroup.Add(1)
	go c.watchForNewNodes(ctx, serviceName)
	return c, nil
}

func (c *Client) Close() error {
	c.cancel()
	for _, client := range c.clients {
		client.Close()
	}
	c.waitGroup.Wait()
	return nil
}

func (c *Client) selectNodes(nodes []Node, maxConnections int) []Node {
	max := len(c.nodesConnected)
	if maxConnections < max {
		max = maxConnections
	}

	// Prefill with exisitng hosts, this also removed dead hosts
	newNodes := make([]Node, maxConnections)
	for _, node := range nodes {
		if _, ok := c.nodesConnected[node]; ok {
			newNodes = append(newNodes, node)
		}
	}

	// Find the new hosts to add
	currConnectionCount := len(c.nodesConnected) - 1
	for i := currConnectionCount; len(newNodes) < max; i++ {
		index := c.hashConnectionIndex(i, len(nodes))
		node := nodes[index]
		newNodes = append(newNodes, node)
	}

	return newNodes
}

func (c *Client) connectToNodes(nodes []Node) ([]*rpc.Client, error) {
	// Add all new nodes that aren't already connected to
	clients := make([]*rpc.Client, len(nodes))
	for i, node := range nodes {
		if _, ok := c.nodesConnected[node]; ok {
			continue
		}
		dialAddr := fmt.Sprintf("%v:%v", node.Address, node.Port)
		client, err := rpc.DialHTTP("tcp", dialAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial service address %v: %w", dialAddr, err)
		}
		clients[i] = client
	}

	return clients, nil
}

func (c *Client) hashConnectionIndex(connNumber, nodeSize int) int {
	h := fnv.New32a()
	h.Write([]byte(c.clientNode + strconv.Itoa(connNumber)))
	return int(h.Sum32()) % nodeSize
}

func (c *Client) watchForNewNodes(ctx context.Context, serviceName string) error {
	defer c.waitGroup.Done()

	nodesChan := c.registry.WatchService(ctx, serviceName)
	for nodes := range nodesChan {
		if len(nodes) == 0 {
			continue
		}

		newNodes := c.selectNodes(nodes, DefaultMaxClientConnections)
		zap.S().Infow("new nodes", "nodes", newNodes)
		clients, err := c.connectToNodes(newNodes)
		if err != nil {
			return err
		}

		nodesConnected := make(map[Node]struct{}, len(newNodes))
		for _, node := range newNodes {
			nodesConnected[node] = struct{}{}
		}

		c.clients = append(c.clients, clients...)
		c.nodesConnected = nodesConnected
	}
	return nil
}

func (c *Client) getConnection(index int) *rpc.Client {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clients[index]
}
