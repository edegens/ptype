package cluster

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

const storePrefix = "store"

type KVStore struct {
	kv clientv3.KV
}

func NewKVStore(ctx context.Context, etcdAddr string) (*KVStore, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &KVStore{
		kv: clientv3.NewKV(c),
	}, nil
}

func (kvs *KVStore) Get(ctx context.Context, key string) (string, error) {
	getres, err := kvs.kv.Get(ctx, key, defaultGetOptions...)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	fmt.Println(getres)
	return "", nil
}
