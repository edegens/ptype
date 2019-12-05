package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"go.etcd.io/etcd/client"
)

const servicesPrefix = "services"

type Node struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

func (i *Node) jsonString() (string, error) {
	bytes, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("failed to marshal node: %w", err)
	}
	return string(bytes), nil
}

type ServiceRegistry struct {
	kapi client.KeysAPI

	services map[string][]Node
	lock     sync.RWMutex
}

func NewServiceRegistry(ctx context.Context, etcdAddr string) (*ServiceRegistry, error) {
	cfg := client.Config{Endpoints: []string{etcdAddr}}
	c, err := client.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	sr := &ServiceRegistry{
		kapi:     client.NewKeysAPI(c),
		services: make(map[string][]Node),
	}
	go sr.watcher(ctx)

	return sr, nil
}

func (sr *ServiceRegistry) Register(ctx context.Context, serviceName, nodeName string, port int) error {
	path := []string{servicesPrefix, serviceName, nodeName}
	key := strings.Join(path, "/")

	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to read hostname: %w", err)
	}

	node := Node{
		Address: host,
		Port:    port,
	}
	val, err := node.jsonString()
	if err != nil {
		return err
	}

	if _, err = sr.kapi.Set(ctx, key, val, nil); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	return sr.syncAll(ctx)
}

func (sr *ServiceRegistry) Services() []string {
	sr.lock.RLock()
	defer sr.lock.RUnlock()

	services := make([]string, 0, len(sr.services))
	for service := range sr.services {
		services = append(services, service)
	}
	return services
}

func (sr *ServiceRegistry) Nodes(serviceName string) []Node {
	sr.lock.RLock()
	defer sr.lock.RUnlock()
	return sr.services[serviceName]
}

var defaultGetOptions = &client.GetOptions{
	Recursive: true,
	Sort:      true,
	Quorum:    true,
}

func (sr *ServiceRegistry) syncAll(ctx context.Context) error {
	res, err := sr.kapi.Get(ctx, servicesPrefix, defaultGetOptions)
	if err != nil {
		return fmt.Errorf("failed to get services from etcd: %w", err)
	}

	services := res.Node.Nodes
	updatedServices := make(map[string][]Node, len(services))
	for _, service := range services {
		nodes := service.Nodes
		updatedServices[service.Key] = make([]Node, len(nodes))

		for index, node := range nodes {
			ins := updatedServices[service.Key][index]

			if err := json.Unmarshal([]byte(node.Value), &ins); err != nil {
				return fmt.Errorf("failed to unmarshal service nodes: %w", err)
			}
		}
	}

	sr.lock.Lock()
	sr.services = updatedServices
	sr.lock.Unlock()

	return nil
}

func (sr *ServiceRegistry) watcher(ctx context.Context) {
	watcher := sr.kapi.Watcher(servicesPrefix, &client.WatcherOptions{Recursive: true})

	for {
		res, err := watcher.Next(ctx)
		switch err {
		case context.Canceled:
			return
		case nil:
		default:
			// TODO: actually report err
			log.Println(err.Error())
		}

		if res == nil {
			continue
		}

		// TODO: Do we want to resync the whole /service path when there is a update?
		// Or do we want to reply that action on our cache? The downside being that the reply
		// requires a lot of rebuilding.
		log.Println(res.Node.String())
	}
}
