package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockRegistry struct {
	services map[string][]Node
	err      error
	nodeChan chan []Node
}

func newMockRegistry(initialNodes []Node) mockRegistry {
	mock := mockRegistry{
		nodeChan: make(chan []Node),
	}
	go func() { mock.nodeChan <- initialNodes }()
	return mock
}

func (m *mockRegistry) Register(ctx context.Context, serviceName, nodeName, host string, port int) error {
	return m.err
}

func (m *mockRegistry) Services(ctx context.Context) (map[string][]Node, error) {
	return m.services, m.err
}

func (m *mockRegistry) WatchService(ctx context.Context, serviceName string) chan []Node {
	return m.nodeChan
}

type RPCTest int

func (r *RPCTest) Call(x string, y *string) error {
	*y = "who's " + x
	return nil
}

func (r *RPCTest) Go(x string, y *string) error {
	time.Sleep(time.Second)
	*y = "who's " + x
	return nil
}

type RPCRetryTest struct {
	called          uint32
	callsBeforePass uint32
}

func (r *RPCRetryTest) Call(arg string, called *int) error {
	atomic.AddUint32(&r.called, 1)
	if r.called >= r.callsBeforePass {
		*called = int(r.called)
		return nil
	}
	return errors.New("failed")
}

func (r *RPCRetryTest) Go(arg string, called *int) error {
	time.Sleep(time.Second)
	atomic.AddUint32(&r.called, 1)
	if r.called >= r.callsBeforePass {
		*called = int(r.called)
		return nil
	}
	return errors.New("failed")
}

var testConnConfig = &ConnConfig{
	MaxConnections:     DefaultConnConfig.MaxConnections,
	InitialNodeTimeout: time.Second,
	DebounceTime:       time.Second,
}

func TestClient_default_conn_config(t *testing.T) {
	mock := newMockRegistry([]Node{})
	c, err := newClient("", "foo", &mock, nil)
	require.NoError(t, err)
	require.Equal(t, DefaultConnConfig, c.conns.cfg)
}

func TestClient_Call(t *testing.T) {
	ts := rpc.NewServer()
	err := ts.Register(new(RPCTest))
	require.NoError(t, err)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() { _ = http.Serve(l, ts) }()

	node := Node{
		Address: "127.0.0.1",
		Port:    l.Addr().(*net.TCPAddr).Port,
	}
	mock := newMockRegistry([]Node{node})
	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var reply string
	err = c.Call("RPCTest.Call", "joe", &reply)
	require.NoError(t, err)
	require.Equal(t, "who's joe", reply)
}

func TestClient_Call_with_retry_multi_node(t *testing.T) {
	ts1 := rpc.NewServer()
	err := ts1.Register(&RPCRetryTest{callsBeforePass: 2})
	require.NoError(t, err)

	ts2 := rpc.NewServer()
	err = ts2.Register(&RPCRetryTest{callsBeforePass: 0})
	require.NoError(t, err)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l1, ts1) }()

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l2, ts2) }()

	mock := newMockRegistry([]Node{
		{
			Address: "127.0.0.1",
			Port:    l1.Addr().(*net.TCPAddr).Port,
		},
		{
			Address: "127.0.0.1",
			Port:    l2.Addr().(*net.TCPAddr).Port,
		},
	})

	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var calls int
	err = c.Call("RPCRetryTest.Call", "", &calls)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestClient_Call_with_retry_error(t *testing.T) {
	ts1 := rpc.NewServer()
	err := ts1.Register(&RPCRetryTest{callsBeforePass: 10})
	require.NoError(t, err)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l1, ts1) }()

	mock := newMockRegistry([]Node{
		{
			Address: "127.0.0.1",
			Port:    l1.Addr().(*net.TCPAddr).Port,
		},
	})

	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var calls int
	err = c.Call("RPCRetryTest.Call", "", &calls)
	require.Error(t, err)
	require.Equal(t, 0, calls)
}

func TestClient_Go(t *testing.T) {
	ts := rpc.NewServer()
	err := ts.Register(new(RPCTest))
	require.NoError(t, err)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() { _ = http.Serve(l, ts) }()

	node := Node{
		Address: "127.0.0.1",
		Port:    l.Addr().(*net.TCPAddr).Port,
	}
	mock := newMockRegistry([]Node{node})
	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var reply string
	call := c.Go("RPCTest.Go", "joe", &reply, nil)
	require.NotNil(t, call)
	require.NoError(t, call.Error)

	<-call.Done
	require.Equal(t, "who's joe", reply)
}

func TestClient_Go_with_retry_multi_node(t *testing.T) {
	ts1 := rpc.NewServer()
	err := ts1.Register(&RPCRetryTest{callsBeforePass: 2})
	require.NoError(t, err)

	ts2 := rpc.NewServer()
	err = ts2.Register(&RPCRetryTest{callsBeforePass: 0})
	require.NoError(t, err)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l1, ts1) }()

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l2, ts2) }()

	mock := newMockRegistry([]Node{
		{
			Address: "127.0.0.1",
			Port:    l1.Addr().(*net.TCPAddr).Port,
		},
		{
			Address: "127.0.0.1",
			Port:    l2.Addr().(*net.TCPAddr).Port,
		},
	})

	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var calls int
	call := c.Go("RPCRetryTest.Go", "", &calls, nil)
	require.NotNil(t, call)
	require.NoError(t, call.Error)

	<-call.Done
	require.Equal(t, 1, calls)
}

func TestClient_Go_with_retry_error(t *testing.T) {
	ts1 := rpc.NewServer()
	err := ts1.Register(&RPCRetryTest{callsBeforePass: 10})
	require.NoError(t, err)

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l1.Close()

	go func() { _ = http.Serve(l1, ts1) }()

	mock := newMockRegistry([]Node{
		{
			Address: "127.0.0.1",
			Port:    l1.Addr().(*net.TCPAddr).Port,
		},
	})

	c, err := newClient("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	var calls int
	call := c.Go("RPCRetryTest.Go", "", &calls, nil)
	require.NotNil(t, call)
	require.NoError(t, call.Error)

	resp := <-call.Done
	require.Error(t, resp.Error)
	require.Equal(t, 0, calls)
}

func TestNewConnectionBalancer_successul_initial_connect(t *testing.T) {
	node, close := testRPCServer(t)
	defer close()

	mock := newMockRegistry([]Node{node})
	c, err := newConnectionBalancer("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, []Node{node}, c.getSelectedNodes())
	require.Empty(t, c.errChan)
}

func TestNewConnectionBalancer_with_no_available_server(t *testing.T) {
	mock := mockRegistry{
		nodeChan: make(chan []Node),
	}
	c, err := newConnectionBalancer("", "foo", &mock, testConnConfig)
	require.Error(t, err)
	require.Nil(t, c)
}

func TestNewConnectionBalancer_with_servers_failing_to_connect(t *testing.T) {
	mock := newMockRegistry([]Node{{Address: "127.0.0.1", Port: 1234}})
	c, err := newConnectionBalancer("", "foo", &mock, testConnConfig)
	require.Error(t, err)
	require.Nil(t, c)
}

func TestConnectionBalancer_watchForNewNodes(t *testing.T) {
	node, close := testRPCServer(t)
	defer close()
	mock := newMockRegistry([]Node{node})

	c, err := newConnectionBalancer("", "foo", &mock, testConnConfig)
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, []Node{node}, c.getSelectedNodes())

	node2, close := testRPCServer(t)
	defer close()
	node3, close := testRPCServer(t)
	defer close()
	node4, close := testRPCServer(t)
	defer close()

	t.Run("test when more than max connections of nodes get sent", func(t *testing.T) {
		go func() {
			mock.nodeChan <- []Node{node, node2, node3, node4}
		}()

		select {
		case err := <-c.errChan:
			require.NoError(t, err)
		default:
		}

		<-c.connsUpdated
		require.Equal(t, []Node{node4, node, node2}, c.getSelectedNodes())
	})

	t.Run("test when connected node is removed", func(t *testing.T) {
		go func() {
			mock.nodeChan <- []Node{node, node3, node4}
		}()

		select {
		case err := <-c.errChan:
			require.NoError(t, err)
		default:
		}

		<-c.connsUpdated
		require.Equal(t, []Node{node, node3, node4}, c.getSelectedNodes())
	})

	t.Run("test debounce of new nodes", func(t *testing.T) {
		go func() {
			mock.nodeChan <- []Node{node}
			mock.nodeChan <- []Node{node2}
			mock.nodeChan <- []Node{node3}
			mock.nodeChan <- []Node{node, node2, node3}
		}()

		select {
		case err := <-c.errChan:
			require.NoError(t, err)
		default:
		}

		<-c.connsUpdated
		require.Equal(t, []Node{node, node2, node3}, c.getSelectedNodes())
	})
}

func TestConnectionBalancer_roundRobinSelect(t *testing.T) {
	clients := make([]*rpc.Client, 100)
	for i := range clients {
		clients[i] = &rpc.Client{}
	}

	c := connectionBalancer{
		clients: clients,
	}
	for i := range clients {
		client := c.roundRobinSelect()
		require.Equal(t, clients[i], client)
	}

	t.Run("test for overflow", func(t *testing.T) {
		client := c.roundRobinSelect()
		require.Equal(t, clients[0], client)
	})

	t.Run("test for conccurency", func(t *testing.T) {
		clientChan := make(chan *rpc.Client, len(clients))
		go func() {
			for range clients {
				clientChan <- c.roundRobinSelect()
			}
		}()

		set := map[*rpc.Client]struct{}{}
		for range clients {
			client := <-clientChan
			_, ok := set[client]
			require.False(t, ok)
			set[client] = struct{}{}
		}
	})
}

func TestConnectionBalancer_mesh_network(t *testing.T) {
	node, close := testRPCServer(t)
	defer close()
	node2, close := testRPCServer(t)
	defer close()
	mock := newMockRegistry([]Node{node, node2})

	cfg := &ConnConfig{
		MaxConnections:     0,
		InitialNodeTimeout: testConnConfig.InitialNodeTimeout,
		DebounceTime:       testConnConfig.DebounceTime,
	}
	c, err := newConnectionBalancer("", "foo", &mock, cfg)
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, []Node{node, node2}, c.getSelectedNodes())

	node3, close := testRPCServer(t)
	defer close()
	node4, close := testRPCServer(t)
	defer close()

	go func() {
		mock.nodeChan <- []Node{node, node2, node3}
	}()

	select {
	case err := <-c.errChan:
		require.NoError(t, err)
	default:
	}

	<-c.connsUpdated
	require.Equal(t, []Node{node, node2, node3}, c.getSelectedNodes())

	go func() {
		mock.nodeChan <- []Node{node, node2, node3, node4}
	}()

	select {
	case err := <-c.errChan:
		require.NoError(t, err)
	default:
	}

	<-c.connsUpdated
	// all nodes are connected to
	require.Equal(t, []Node{node, node2, node3, node4}, c.getSelectedNodes())
}

func testRPCServer(t *testing.T) (Node, func() error) {
	ts := rpc.NewServer()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = http.Serve(l, ts) }()

	n := Node{
		Address: "127.0.0.1",
		Port:    l.Addr().(*net.TCPAddr).Port,
	}

	return n, l.Close
}
