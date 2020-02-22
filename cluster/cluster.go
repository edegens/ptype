package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

type Cluster struct {
	Registry Registry
	Store    *KVStore
	client   *clientv3.Client
	etcd     *embed.Etcd
	host     string
}

func Join(ctx context.Context, cfg Config) (*Cluster, error) {
	if cfg.Debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize logger: %w", err)
		}
		zap.ReplaceGlobals(logger)
	}

	e, err := startEmbeddedEtcd(cfg.etcdConfig)
	if err != nil {
		return nil, err
	}

	clientURL := cfg.etcdConfig.LCUrls[0].String()
	registry, err := newEtcdRegistry(ctx, clientURL)
	if err != nil {
		return nil, err
	}

	host, err := hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to read hostname: %w", err)
	}
	if err := registry.Register(ctx, cfg.ServiceName, cfg.NodeName, host, cfg.Port); err != nil {
		return nil, err
	}

	clientCfg := clientv3.Config{
		Endpoints:   []string{clientURL},
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from config: %w", err)
	}

	return &Cluster{
		Registry: registry,
		client:   client,
		etcd:     e,
		host:     host,
	}, nil
}

type MemberAddInfo struct {
	InitialCluster      string
	InitialClusterState string
}

func (c *Cluster) MemberAdd(ctx context.Context, name, peerURL string) (*MemberAddInfo, error) {
	mresp, err := c.client.MemberAdd(ctx, []string{peerURL})
	if err != nil {
		return nil, fmt.Errorf("failed to add member with peerURL n%v: %w", peerURL, err)
	}

	initialClusterStrings := make([]string, 0, 2)
	for _, member := range mresp.Members {
		if member.Name == "" && strings.Compare(peerURL, member.PeerURLs[0]) == 0 {
			initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(name, member.PeerURLs[0]))
		} else {
			initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(member.Name, member.PeerURLs[0]))
		}
	}
	sort.Strings(initialClusterStrings)

	initialCluster := strings.Join(initialClusterStrings, ",")

	mai := &MemberAddInfo{
		InitialCluster:      initialCluster,
		InitialClusterState: "existing",
	}

	return mai, nil
}

func (c *Cluster) MemberList(ctx context.Context) ([]*etcdserverpb.Member, error) {
	resp, err := c.client.MemberList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve member list: %w", err)
	}

	return resp.Members, nil
}

func initialClusterStringFormatter(name, peerURL string) string {
	return fmt.Sprintf("%s=%s", name, peerURL)
}

func (c *Cluster) Close() error {
	c.etcd.Close()
	<-c.etcd.Server.StopNotify()
	return nil
}

func (c *Cluster) NewClient(serviceName string) (*Client, error) {
	return newClient(c.host, serviceName, c.Registry)
}

func startEmbeddedEtcd(cfg *embed.Config) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	<-e.Server.ReadyNotify()
	return e, nil
}

func hostname() (string, error) {
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
