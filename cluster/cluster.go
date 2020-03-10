package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

type Cluster struct {
	Registry  Registry
	Store     *KVStore
	client    *clientv3.Client
	etcd      *embed.Etcd
	localAddr string
}

func Join(ctx context.Context, cfg Config) (*Cluster, error) {
	if cfg.Debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize logger: %w", err)
		}
		zap.ReplaceGlobals(logger)
	}

	//clientUrl := cfg.etcdConfig.LCUrls[0].String()
    clientUrl := "http://127.0.0.1:12379"
	clientCfg := clientv3.Config{
		Endpoints:   []string{clientUrl},
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from config: %w", err)
	}

	if (cfg.etcdConfig.ClusterState == embed.ClusterStateFlagExisting) {
		if err := memberAdd(ctx, client, cfg.etcdConfig.LPUrls); err != nil {
			return nil, err
		}
        time.Sleep(10*time.Second)
	}

	e, err := startEmbeddedEtcd(cfg.etcdConfig)
	if err != nil {
		return nil, err
	}

	registry, err := newEtcdRegistry(ctx, clientUrl)
	if err != nil {
		return nil, err
	}

	addr, err := getIP()
	if err != nil {
		return nil, fmt.Errorf("failed to read hostname: %w", err)
	}
	if err := registry.Register(ctx, cfg.ServiceName, cfg.NodeName, addr, cfg.Port); err != nil {
		return nil, err
	}

	return &Cluster{
		Registry:  registry,
		client:    client,
		etcd:      e,
		localAddr: addr,
	}, nil
}

func memberAdd(ctx context.Context, client *clientv3.Client, peerUrls []url.URL) error {
	peerUrlStrings := make([]string, len(peerUrls))
	for i, url := range peerUrls {
		peerUrlStrings[i] = url.String()
	}

	_, err := client.MemberAdd(ctx, peerUrlStrings)
	if err != nil {
		return fmt.Errorf("failed to add member with peerURLs %v: %w", peerUrls, err)
	}

	return nil
}

func (c *Cluster) MemberList(ctx context.Context) ([]*etcdserverpb.Member, error) {
	resp, err := c.client.MemberList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve member list: %w", err)
	}

	return resp.Members, nil
}

func (c *Cluster) Close() error {
	c.etcd.Close()
	<-c.etcd.Server.StopNotify()
	return nil
}

func (c *Cluster) NewClient(serviceName string) (*Client, error) {
	return newClient(c.localAddr, serviceName, c.Registry)
}

func startEmbeddedEtcd(cfg *embed.Config) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	<-e.Server.ReadyNotify()
	return e, nil
}

func getIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to lookup interface addrs: %w", err)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.New("no network address that aren't loopbacks")
}
