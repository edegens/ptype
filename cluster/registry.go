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
	kv      clientv3.KV
	watcher clientv3.Watcher
	cli     *clientv3.Client
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
		kv:      clientv3.NewKV(c),
		watcher: clientv3.NewWatcher(c),
		cli:     c,
	}, nil
}

func (er *etcdRegistry) Register(ctx context.Context, serviceName, nodeName, host string, port int) error {
	node := Node{Address: host, Port: port}
	val, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}

	// TODO: Discuss appripriate lease time, I chose 2 seconds using no imperical data
	resp, err := er.cli.Grant(ctx, 2)
	if err != nil {
		return fmt.Errorf("failed to create lease for service: %w", err)
	}

	key := filepath.Join(servicesPrefix, serviceName, nodeName)
	if _, err = er.kv.Put(ctx, key, string(val), clientv3.WithLease(resp.ID)); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	keepAliveResp, err := er.cli.KeepAlive(ctx, resp.ID)
	if err != nil {
		return fmt.Errorf("failed to keep service registered: %w", err)
	}
	go func() {
		for {
			kar := <-keepAliveResp
			if kar == nil {
				fmt.Printf("service %s failed to refresh \n", serviceName)
				break

			}
			fmt.Printf("service %s refreshed and vaild for(ttl): %d \n", serviceName, kar.TTL)
		}
	}()

	return nil
}

var defaultGetOptions = []clientv3.OpOption{
	clientv3.WithPrefix(),
	clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
}

func (er *etcdRegistry) Services(ctx context.Context) (map[string][]Node, error) {
	res, err := er.kv.Get(ctx, servicesPrefix, defaultGetOptions...)
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
			if ctx.Err() != nil {
				return
			}

			nodes, err := er.nodes(ctx, serviceName)
			if err != nil {
				continue
			}
			nodesChan <- nodes

			select {
			case res := <-watchChan:
				if res.Err() != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nodesChan
}

func (er *etcdRegistry) nodes(ctx context.Context, serviceName string) ([]Node, error) {
	key := filepath.Join(servicesPrefix, serviceName)
	res, err := er.kv.Get(ctx, key, defaultGetOptions...)
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
