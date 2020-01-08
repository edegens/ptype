package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const servicesPrefix = "services"

type Node struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type ServiceRegistry struct {
	KV clientv3.KV
}

func NewServiceRegistry(ctx context.Context, etcdAddr string) (*ServiceRegistry, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &ServiceRegistry{
		KV: clientv3.NewKV(c),
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
	if _, err = sr.KV.Put(ctx, key, string(val)); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	return nil
}

func (sr *ServiceRegistry) Services(ctx context.Context) (map[string]Node, error) {
	res, err := sr.KV.Get(ctx, servicesPrefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return nil, fmt.Errorf("failed to get services from etcd: %w", err)
	}

	var node Node
	serviceDir := res.Kvs
	services := make(map[string]Node, len(serviceDir))
	for _, Kvs := range serviceDir {
		if err := json.Unmarshal([]byte(Kvs.Value), &node); err != nil {
			return nil, fmt.Errorf("Failed to unmarshal services nodes: %w", err)
		}
		services[string(Kvs.Key)] = node
	}

	return services, nil
}
