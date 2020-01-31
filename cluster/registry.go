package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const servicesPrefix = "services"

type Registry interface {
	Register(ctx context.Context, serviceName, nodeName, host string, port int) error
	Services(ctx context.Context) (map[string][]Node, error)
	WatchService(ctx context.Context, serviceName string) chan []Node
}

type Node struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type etcdRegistry struct {
	KV      clientv3.KV
	watcher clientv3.Watcher
}

func newEtcdRegistry(ctx context.Context, etcdAddr string) (*etcdRegistry, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &etcdRegistry{
		KV:      clientv3.NewKV(c),
		watcher: clientv3.NewWatcher(c),
	}, nil
}

func (er *etcdRegistry) Register(ctx context.Context, serviceName, nodeName, host string, port int) error {
	node := Node{Address: host, Port: port}
	val, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	key := filepath.Join(servicesPrefix, serviceName, nodeName)
	if _, err = er.KV.Put(ctx, key, string(val)); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	return nil
}

var defaultGetOptions = []clientv3.OpOption{
	clientv3.WithPrefix(),
	clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
}

func (er *etcdRegistry) Services(ctx context.Context) (map[string][]Node, error) {
	res, err := er.KV.Get(ctx, servicesPrefix, defaultGetOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to get services from etcd: %w", err)
	}

	var node Node
	services := make(map[string][]Node)
	for _, kvs := range res.Kvs {
		if err := json.Unmarshal([]byte(kvs.Value), &node); err != nil {
			return nil, fmt.Errorf("failed to unmarshal services nodes: %w", err)
		}

		key := strings.Split(string(kvs.Key), "/")
		if len(key) > 1 {
			service := key[1]
			if _, ok := services[service]; !ok {
				services[service] = make([]Node, 0)
			}
			services[service] = append(services[service], node)
		}
	}

	return services, nil
}

func (er *etcdRegistry) WatchService(ctx context.Context, serviceName string) chan []Node {
	key := filepath.Join(servicesPrefix, serviceName)

	nodesChan := make(chan []Node)
	watchChan := er.watcher.Watch(ctx, key, clientv3.WithPrefix())
	go func() {
		defer close(nodesChan)
		for {
			select {
			case res := <-watchChan:
				if err := res.Err(); err != nil {
					fmt.Printf("watching service %v, error occured: %v", serviceName, err)
					return
				}
				nodes, err := er.nodes(ctx, serviceName)
				if err != nil {
					fmt.Printf("reading nodes for service %v, error occured: %v", serviceName, err)
					continue
				}
				nodesChan <- nodes
			case <-ctx.Done():
				return
			}
		}
	}()

	return nodesChan
}

func (er *etcdRegistry) nodes(ctx context.Context, serviceName string) ([]Node, error) {
	key := filepath.Join(servicesPrefix, serviceName)
	res, err := er.KV.Get(ctx, key, defaultGetOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to get services from etcd: %w", err)
	}

	nodes := make([]Node, len(res.Kvs))
	for i, kvs := range res.Kvs {
		if err := json.Unmarshal([]byte(kvs.Value), &nodes[i]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal services nodes: %w", err)
		}
	}
	return nodes, nil
}
