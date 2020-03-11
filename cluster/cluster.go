package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
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

func Join(ctx context.Context, cfg Config, clientUrls []string) (*Cluster, error) {
	if cfg.Debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize logger: %w", err)
		}
		zap.ReplaceGlobals(logger)
	}

	if cfg.etcdConfig.ClusterState == embed.ClusterStateFlagNew && len(clientUrls) == 0 {
		clientUrls = urlsToString(cfg.etcdConfig.LCUrls)
	} else if cfg.etcdConfig.ClusterState == embed.ClusterStateFlagExisting && len(clientUrls) == 0 {
		return nil, fmt.Errorf("joining an existing cluster requires at least one client url from a member from the existing cluster")
	}

	clientCfg := clientv3.Config{
		Endpoints:   clientUrls,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from config: %w", err)
	}

	if cfg.etcdConfig.ClusterState == embed.ClusterStateFlagExisting {
		cfg.etcdConfig.InitialCluster, err = memberAdd(ctx, client, *cfg.etcdConfig)
		if err != nil {
			return nil, err
		}
	}

	e, err := startEmbeddedEtcd(cfg.etcdConfig)
	if err != nil {
		return nil, err
	}

	registry, err := newEtcdRegistry(ctx, clientUrls)
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

func memberAdd(ctx context.Context, client *clientv3.Client, cfg embed.Config) (string, error) {
	peerUrlStrings := make([]string, len(cfg.LPUrls))
	for i, url := range cfg.LPUrls {
		peerUrlStrings[i] = url.String()
	}

	mresp, err := client.MemberAdd(ctx, peerUrlStrings)
	if err != nil {
		return "", fmt.Errorf("failed to add member with peerURLs %v: %w", cfg.LPUrls, err)
	}

	initialClusterStrings := make([]string, 0, 1+len(cfg.LPUrls))
	for _, url := range cfg.LPUrls {
		initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(cfg.Name, url.String()))
	}
	for _, member := range mresp.Members {
		if member.Name != "" {
			for _, url := range member.PeerURLs {
				initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(member.Name, url))
			}
		}
	}
	sort.Strings(initialClusterStrings)

	initialCluster := strings.Join(initialClusterStrings, ",")

	return initialCluster, nil
}

func initialClusterStringFormatter(name, peerUrl string) string {
	return fmt.Sprintf("%s=%s", name, peerUrl)
}

func urlsToString(urls []url.URL) []string {
	urlStrings := make([]string, len(urls))
	for i, url := range urls {
		urlStrings[i] = url.String()
	}
	return urlStrings
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
