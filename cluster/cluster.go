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

func Join(ctx context.Context, cfg *Config) (*Cluster, error) {
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

	addr, err := getIP()
	if err != nil {
		return nil, fmt.Errorf("failed to read hostname: %w", err)
	}
    if err := validateNodeName(cfg); err != nil {
        return nil, err
    }
	if err := registry.Register(ctx, cfg.ServiceName, cfg.NodeName, addr, cfg.Port); err != nil {
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
		Registry:  registry,
		client:    client,
		etcd:      e,
		localAddr: addr,
	}, nil
}

type MemberAddInfo struct {
	InitialCluster      string
	ClusterState string
}

func (c *Cluster) MemberAdd(ctx context.Context, peerURL, serviceName, nodeName string, port int) (*MemberAddInfo, error) {
	mresp, err := c.client.MemberAdd(ctx, []string{peerURL})
	if err != nil {
        return nil, fmt.Errorf("failed to add member with peerURL %v: %w", peerURL, err)
	}

    addrURL, err := url.Parse(peerURL)
    if err != nil {
        return nil, fmt.Errorf("failed to parse peerURL %v: %w", peerURL, err)
    }
    addr := fmt.Sprintf("%s://%s", addrURL.Scheme, addrURL.Host)

    if err := c.Registry.Register(ctx, serviceName, nodeName, addr, port); err != nil {
        return nil, err
    }

	initialClusterStrings := make([]string, 0, 2)
	for _, member := range mresp.Members {
		if member.Name == "" && strings.Compare(peerURL, member.PeerURLs[0]) == 0 {
			initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(nodeName, member.PeerURLs[0]))
		} else {
			initialClusterStrings = append(initialClusterStrings, initialClusterStringFormatter(member.Name, member.PeerURLs[0]))
		}
	}
	sort.Strings(initialClusterStrings)

	initialCluster := strings.Join(initialClusterStrings, ",")

	mai := &MemberAddInfo{
		InitialCluster:      initialCluster,
		ClusterState: "existing",
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

func validateNodeName(cfg *Config) error {
    if cfg.NodeName == "" && cfg.etcdConfig.Name != "" {
        cfg.NodeName = cfg.etcdConfig.Name
    } else if cfg.NodeName != "" && cfg.etcdConfig.Name == "" {
        cfg.etcdConfig.Name = cfg.NodeName
    } else if cfg.NodeName != cfg.etcdConfig.Name {
        return fmt.Errorf("service config file node name (%v) and node config file node name (%v) do not match", cfg.NodeName, cfg.etcdConfig.Name)
    }

    return nil
}
