package cluster

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

const storePrefix = "store"

var (
	ErrNoKey = errors.New("Key does not exist in store")
)

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

// Get returns the best matched value for the key provided
func (kvs *KVStore) Get(ctx context.Context, key string) (string, error) {
	getres, err := kvs.kv.Get(ctx, fmt.Sprintf("%s/%s", storePrefix, key), defaultGetOptions...)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if len(getres.Kvs) == 0 {
		return "", ErrNoKey
	}
	return string(getres.Kvs[0].Value), nil
}

// GetAll returns all values that correspond to the supplied key
func (kvs *KVStore) GetPrefix(ctx context.Context, key string) ([]string, error) {
	getres, err := kvs.kv.Get(ctx, fmt.Sprintf("%s/%s", storePrefix, key), defaultGetOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if len(getres.Kvs) == 0 {
		return nil, ErrNoKey
	}

	gets := make([]string, 0)
	for _, kvs := range getres.Kvs {
		gets = append(gets, string(kvs.Value))
	}

	return gets, nil
}
