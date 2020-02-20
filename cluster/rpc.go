package cluster

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
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
const defaultMaxConnections = 3
const defaultInitalNodeTimeout = 5 * time.Second

type connectionBalancer struct {
	clientNode    string
	selectedNodes []Node
	clients       []*rpc.Client
	seq           uint64

	cancel    func()
	errChan   chan error
	waitGroup sync.WaitGroup
}

func newConnectionBalancer(host string, serviceName string, r Registry) (*connectionBalancer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &connectionBalancer{
		clientNode: host,
		clients:    []*rpc.Client{},
		cancel:     cancel,
		errChan:    make(chan error, 1),
	}

	nodesChan := r.WatchService(ctx, serviceName)

	var initalNodes []Node
	select {
	case initalNodes = <-nodesChan:
	case <-time.After(defaultInitalNodeTimeout):
		return nil, fmt.Errorf("no inital nodes provided for %v", serviceName)
	}

	if err := c.handleNewNodes(initalNodes); err != nil {
		return nil, err
	}

	c.waitGroup.Add(1)
	go c.watchForNewNodes(ctx, nodesChan)
	return c, nil
}

func (c *connectionBalancer) Get() *rpc.Client {
	return c.roundRobinSelect()
}

func (c *connectionBalancer) roundRobinSelect() *rpc.Client {
	index := atomic.AddUint64(&c.seq, uint64(1))
	return c.clients[int(index)%len(c.clients)]
}

func (c *connectionBalancer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	for _, client := range c.clients {
		client.Close()
	}
	c.waitGroup.Wait()
	close(c.errChan)
	return nil
}

func (c *connectionBalancer) watchForNewNodes(ctx context.Context, nodesChan chan []Node) {
	defer c.waitGroup.Done()

	for {
		select {
		case nodes := <-nodesChan:
			if len(nodes) == 0 {
				continue
			}

			if err := c.handleNewNodes(nodes); err != nil {
				select {
				case c.errChan <- err:
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *connectionBalancer) handleNewNodes(nodes []Node) error {
	selectedNodes := c.selectNodes(nodes, defaultMaxConnections)
	clients, err := c.connectToNodes(selectedNodes)
	if err != nil {
		return node, err
	}

	nodes, ok := serv[serviceName]
	if !ok || len(nodes) == 0 {
		return node, fmt.Errorf("failed to find service %v in the registry", serviceName)
	c.clients = clients
	c.selectedNodes = selectedNodes
	return nil
}

// selectedNodes selects a number of nodes from the list of nodes given.
// It uses a hashing scheme to determine which index the current node should select.
// We select up to a max number of connections or what the size of the nodes allows.
// The current hashing scheme is such:
//		index n = hash(host + n)
// Where n the connection number being made.
func (c *connectionBalancer) selectNodes(nodes []Node, maxConnections int) []Node {
	if len(nodes) <= maxConnections {
		return nodes
	}

	selectedNodes := make([]Node, 0, maxConnections)
	for i := 0; len(selectedNodes) < maxConnections; i++ {
		index := c.hashConnectionIndex(i, len(nodes))
		selectedNodes = append(selectedNodes, nodes[index])
	}

	return selectedNodes
}

func (c *connectionBalancer) hashConnectionIndex(connNumber, nodeSize int) int {
	h := fnv.New32a()
	h.Write([]byte(c.clientNode + strconv.Itoa(connNumber)))
	return int(h.Sum32()) % nodeSize
}

func (c *connectionBalancer) connectToNodes(nodes []Node) ([]*rpc.Client, error) {
	clients := make([]*rpc.Client, len(nodes))

	for i, node := range nodes {
		dialAddr := fmt.Sprintf("%v:%v", node.Address, node.Port)
		client, err := rpc.DialHTTP("tcp", dialAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial service address %v: %w", dialAddr, err)
		}
		clients[i] = client
	}

	return clients, nil
}
