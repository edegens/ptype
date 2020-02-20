package cluster

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockRegistry struct {
	services map[string][]Node
	err      error
	nodeChan chan []Node
}

func newMockRegistry(initalNodes []Node) mockRegistry {
	mock := mockRegistry{
		nodeChan: make(chan []Node),
	}
	go func() { mock.nodeChan <- initalNodes }()
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

func TestNewClient(t *testing.T) {
	rpc.HandleHTTP()
	ts := http.Server{}
	defer ts.Close()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = ts.Serve(l) }()

	mock := mockRegistry{
		services: map[string][]Node{
			"foo": {
				{
					Address: "127.0.0.1",
					Port:    l.Addr().(*net.TCPAddr).Port,
				},
			},
		},
	}

	client, err := NewClient("foo", &mock)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNewConnectionBalancer_successul_inital_connect(t *testing.T) {
	node, close := testRPCServer(t)
	defer close()

	mock := newMockRegistry([]Node{node})
	c, err := newConnectionBalancer("", "foo", &mock)
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, []Node{node}, c.selectedNodes)
	require.Empty(t, c.errChan)
}

func TestNewConnectionBalancer_with_no_available_server(t *testing.T) {
	mock := mockRegistry{
		nodeChan: make(chan []Node),
	}
	c, err := newConnectionBalancer("", "foo", &mock)
	require.Error(t, err)
	require.Nil(t, c)
}

func TestNewConnectionBalancer_with_servers_failing_to_connect(t *testing.T) {
	mock := newMockRegistry([]Node{{Address: "127.0.0.1", Port: 1234}})
	c, err := newConnectionBalancer("", "foo", &mock)
	require.Error(t, err)
	require.Nil(t, c)
}

func TestConnectionBalancer_watchForNewNodes(t *testing.T) {
	node, close := testRPCServer(t)
	defer close()
	mock := newMockRegistry([]Node{node})

	c, err := newConnectionBalancer("", "foo", &mock)
	require.NoError(t, err)
	defer c.Close()

	require.Equal(t, []Node{node}, c.selectedNodes)

	go func() {
		err := <-c.errChan
		require.NoError(t, err)
	}()

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
		time.Sleep(time.Millisecond)
		require.Equal(t, []Node{node4, node, node2}, c.selectedNodes)
	})

	t.Run("test when connected node is removed", func(t *testing.T) {
		go func() {
			mock.nodeChan <- []Node{node, node3, node4}
		}()
		time.Sleep(time.Millisecond)
		require.Equal(t, []Node{node, node3, node4}, c.selectedNodes)
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
			for _ = range clients {
				clientChan <- c.roundRobinSelect()
			}
		}()

		set := map[*rpc.Client]struct{}{}
		for _ = range clients {
			client := <-clientChan
			_, ok := set[client]
			require.False(t, ok)
			set[client] = struct{}{}
		}
	})
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
