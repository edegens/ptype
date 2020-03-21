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

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
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

	if cfg.etcdConfig.ClusterState == embed.ClusterStateFlagExisting {
		initialCluster, err := joinExistingCluster(ctx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.etcdConfig.InitialCluster = initialCluster
	}

	e, err := startEmbeddedEtcd(ctx, cfg)
	if err != nil {
		return nil, err
	}

	clientUrls := urlsToString(cfg.etcdConfig.LCUrls)
	localClient, err := clientv3.New(clientv3.Config{
		Endpoints:   clientUrls,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from config: %w", err)
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
		client:    localClient,
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

func (c *Cluster) NewClient(serviceName string, cfg *ConnConfig) (*Client, error) {
	return newClient(c.localAddr, serviceName, c.Registry, cfg)
}

func joinExistingCluster(ctx context.Context, cfg Config) (initalCluster string, err error) {
	if len(cfg.InitialClusterClientUrls) == 0 {
		return "", fmt.Errorf("joining an existing cluster requires at least one client url from a member from the existing cluster")
	}
	initialClusterClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.InitialClusterClientUrls,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create etcd client from config: %w", err)
	}

	return memberAdd(ctx, initialClusterClient, *cfg.etcdConfig)
}

func memberAdd(ctx context.Context, client *clientv3.Client, cfg embed.Config) (string, error) {
	peerUrlStrings := make([]string, len(cfg.LPUrls))
	for i, url := range cfg.LPUrls {
		peerUrlStrings[i] = url.String()
	}

	mresp, err := client.MemberAddAsLearner(ctx, peerUrlStrings)
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

func startEmbeddedEtcd(ctx context.Context, cfg Config) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg.etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	<-e.Server.ReadyNotify()
	zap.S().Debugw("etcd started", "id", e.Server.ID(), "learner", e.Server.IsLearner())
	if !e.Server.IsLearner() {
		return e, nil
	}

	initialClusterClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.InitialClusterClientUrls,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from config: %w", err)
	}
	for {
		_, err := initialClusterClient.MemberPromote(ctx, uint64(e.Server.ID()))
		if err == nil {
			zap.S().Debugw("etcd learner successfully promoted", "id", e.Server.ID())
			return e, nil
		}
		if err.Error() == etcdserver.ErrLearnerNotReady.Error() {
			zap.S().Debugw("learner not ready to be promoted", "id", e.Server.ID())
			time.Sleep(2 * time.Second)
			continue
		}
		return nil, fmt.Errorf("can't promote member: %w", err)
	}
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
