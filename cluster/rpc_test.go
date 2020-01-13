package cluster

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockRegistry struct {
	services map[string][]Node
	err      error
}

func (m *mockRegistry) Register(ctx context.Context, serviceName, nodeName, host string, port int) error {
	return m.err
}

func (m *mockRegistry) Services(ctx context.Context) (map[string][]Node, error) {
	return m.services, m.err
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

func TestNewClient_with_no_available_server(t *testing.T) {
	mock := mockRegistry{
		services: map[string][]Node{},
	}

	client, err := NewClient("foo", &mock)
	require.Error(t, err)
	require.Nil(t, client)
}

func TestNewClient_with_servers_failing_to_connect(t *testing.T) {
	mock := mockRegistry{
		services: map[string][]Node{
			"foo": {
				{Address: "127.0.0.1", Port: 1234},
			},
		},
	}

	client, err := NewClient("foo", &mock)
	require.Error(t, err)
	require.Nil(t, client)
}