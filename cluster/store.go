package cluster

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
)

type KVStore struct {
	kv clientv3.KV
}

func NewKVStore(ctx context.Context, etcdAddr string) (*KVStore, error) {
	cfg := clientv3.Config{Endpoints: []string{etcdAddr}}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &KVStore{
		kv: clientv3.NewKV(c),
	}, nil
}
