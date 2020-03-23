package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoClientAvailable = errors.New("no client nodes available")
)

type ConnConfig struct {
	// Max connections to unique nodes in the cluster. If the value 0 is used, then
	// a mesh network will be formed (connection with every node).
	MaxConnections int
	// Timeout for establishing the initial connection with the service's nodes.
	// Connections afterwards are done asynchronously and don't have a timeout.
	InitialNodeTimeout time.Duration
	// Duration to batch the latest node changes. Prevents thundering herd of changes.
	DebounceTime time.Duration
	// Retries is the number of times a request is attempted to get a success. The
	// retires are possibliy done on different nodes.
	Retries int
}

var DefaultConnConfig = &ConnConfig{
	MaxConnections:     3,
	InitialNodeTimeout: 5 * time.Second,
	DebounceTime:       3 * time.Second,
	Retries:            2,
}

type Client struct {
	conns *connectionBalancer
	cfg   *ConnConfig
}

func newClient(host, serviceName string, r Registry, cfg *ConnConfig) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConnConfig
	}
	conns, err := newConnectionBalancer(host, serviceName, r, cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		conns: conns,
		cfg:   cfg,
	}, nil
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return withRetry(c.cfg.Retries, func() error {
		client := c.conns.Get()
		if client == nil {
			return ErrNoClientAvailable
		}
		return client.Call(serviceMethod, args, reply)
	})
}

func (c *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) *rpc.Call {
	doneWrapper := make(chan *rpc.Call, 10)

	if done == nil {
		done = make(chan *rpc.Call, 10)
	}
	call := &rpc.Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}

	go func() {
		err := withRetry(c.cfg.Retries, func() error {
			client := c.conns.Get()
			if client == nil {
				return ErrNoClientAvailable
			}
			client.Go(serviceMethod, args, reply, doneWrapper)

			c := <-doneWrapper
			if call.Error == nil {
				done <- c
				return nil
			}
			return c.Error
		})

		if err != nil {
			call.Error = err
			done <- call
		}
	}()

	return call
}

func withRetry(maxRetries int, f func() error) error {
	for {
		retries := 0
		err := f()
		if err == nil || retries >= maxRetries {
			return err
		}
		retries++
	}
}

func (c *Client) Close() error {
	return c.conns.Close()
}

func (c *Client) ConnectionErrs() chan error {
	return c.conns.errChan
}

type connectionBalancer struct {
	cfg       *ConnConfig
	localAddr string
	seq       uint64

	selectedNodes []Node
	clients       []*rpc.Client
	connsUpdated  chan struct{}
	lock          sync.RWMutex

	cancel    func()
	errChan   chan error
	waitGroup sync.WaitGroup
}

func newConnectionBalancer(host, serviceName string, r Registry, cfg *ConnConfig) (*connectionBalancer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &connectionBalancer{
		cfg:          cfg,
		localAddr:    host,
		clients:      []*rpc.Client{},
		connsUpdated: make(chan struct{}, 5),
		cancel:       cancel,
		errChan:      make(chan error, 1),
	}

	nodesChan := r.WatchService(ctx, serviceName)

	var initialNodes []Node
	select {
	case initialNodes = <-nodesChan:
	case <-time.After(c.cfg.InitialNodeTimeout):
		return nil, fmt.Errorf("no initial nodes provided for %v", serviceName)
	}

	if err := c.handleNewNodes(initialNodes); err != nil {
		return nil, err
	}
	<-c.connsUpdated // consume the first update message

	c.waitGroup.Add(1)
	go c.watchForNewNodes(ctx, nodesChan)
	return c, nil
}

func (c *connectionBalancer) Get() *rpc.Client {
	return c.roundRobinSelect()
}

func (c *connectionBalancer) roundRobinSelect() *rpc.Client {
	clients := c.getClients()
	if len(clients) == 0 {
		return nil
	}
	index := atomic.AddUint64(&c.seq, uint64(1))
	return clients[int(index)%len(clients)]
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

	var newNodes []Node
	for {
		select {
		case nodes := <-nodesChan:
			if len(nodes) == 0 {
				continue
			}
			newNodes = nodes
			continue
		case <-time.After(c.cfg.DebounceTime):
			if newNodes == nil {
				continue
			}
			if err := c.handleNewNodes(newNodes); err != nil {
				select {
				case c.errChan <- err:
				default:
				}
			}
			newNodes = nil
		case <-ctx.Done():
			return
		}
	}
}

func (c *connectionBalancer) handleNewNodes(nodes []Node) error {
	selectedNodes := c.selectNodes(nodes, c.cfg.MaxConnections)
	clients, err := c.connectToNodes(selectedNodes)
	if err != nil {
		return err
	}

	c.lock.Lock()
	c.clients = clients
	c.selectedNodes = selectedNodes
	c.lock.Unlock()

	select {
	case c.connsUpdated <- struct{}{}:
	default:
	}

	return nil
}

// selectedNodes selects a number of nodes from the list of nodes given.
// It uses a hashing scheme to determine which index the current node should select.
// We select up to a max number of connections or what the size of the nodes allows.
// The current hashing scheme is such:
//		index n = hash(host + n)
// Where n the connection number being made.
func (c *connectionBalancer) selectNodes(nodes []Node, maxConnections int) []Node {
	if len(nodes) <= maxConnections || maxConnections == 0 {
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
	h.Write([]byte(c.localAddr + strconv.Itoa(connNumber)))
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

func (c *connectionBalancer) getClients() []*rpc.Client {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clients
}

func (c *connectionBalancer) getSelectedNodes() []Node {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.selectedNodes
}
