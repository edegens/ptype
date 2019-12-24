package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/client"
)

const servicesPrefix = "services"

type Node struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type ServiceRegistry struct {
	kapi client.KeysAPI
}

func NewServiceRegistry(ctx context.Context, etcdAddr string) (*ServiceRegistry, error) {
	cfg := client.Config{Endpoints: []string{etcdAddr}}
	c, err := client.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &ServiceRegistry{
		kapi: client.NewKeysAPI(c),
	}, nil
}

func (sr *ServiceRegistry) Register(ctx context.Context, serviceName, nodeName string, port int) error {
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to read hostname: %w", err)
	}

	node := Node{Address: host, Port: port}
	val, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := filepath.Join(servicesPrefix, serviceName, nodeName)
	if _, err = sr.kapi.Set(ctx, key, string(val), nil); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	return nil
}

var defaultGetOptions = &client.GetOptions{
	Recursive: true,
	Sort:      true,
}

func (sr *ServiceRegistry) Services(ctx context.Context) (map[string][]Node, error) {
	res, err := sr.kapi.Get(ctx, servicesPrefix, defaultGetOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to get services from etcd: %w", err)
	}

	serviceDir := res.Node.Nodes
	services := make(map[string][]Node, len(serviceDir))
	for _, service := range serviceDir {
		nodes := service.Nodes
		_, key := filepath.Split(service.Key)
		services[key] = make([]Node, 0, len(nodes))

		for _, node := range nodes {
			var n Node
			if err := json.Unmarshal([]byte(node.Value), &n); err != nil {
				return nil, fmt.Errorf("failed to unmarshal service nodes: %w", err)
			}
			services[key] = append(services[key], n)
		}
	}

	return services, nil
}
