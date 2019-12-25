package cluster

import (
	"context"
	"fmt"
	"os"

	"go.etcd.io/etcd/embed"
)

type Cluster struct {
	Registry *ServiceRegistry
}

func Join(ctx context.Context, cfg Config) (*Cluster, error) {
	if _, err := startEmbeddedEtcd(cfg.etcdConfig); err != nil {
		return nil, err
	}

	clientURL := cfg.etcdConfig.LCUrls[0].String()
	registry, err := NewServiceRegistry(ctx, clientURL)
	if err != nil {
		return nil, err
	}

	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to read hostname: %w", err)
	}
	if err := registry.Register(ctx, cfg.ServiceName, cfg.NodeName, host, cfg.Port); err != nil {
		return nil, err
	}

	return &Cluster{registry}, nil
}

func startEmbeddedEtcd(cfg *embed.Config) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	<-e.Server.ReadyNotify()
	return e, nil
}
