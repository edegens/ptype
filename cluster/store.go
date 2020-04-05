package cluster

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"path/filepath"
	"time"
)

const storePrefix = "store"

var (
	ErrNoKey = errors.New("Key could not be found")
)

type KVStore struct {
	kv clientv3.KV
}

func newKVStore(ctx context.Context, etcdAddrs []string) (*KVStore, error) {
	cfg := clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addresses %v: %w", etcdAddrs, err)
	}

	return &KVStore{
		kv: clientv3.NewKV(c),
	}, nil
}

// Get returns the best matched value for the key provided
func (kvs *KVStore) Get(ctx context.Context, key string, options ...clientv3.OpOption) ([]string, error) {
	fetched := make([]string, 0)
	getres, err := kvs.kv.Get(ctx, filepath.Join(storePrefix, key), options...)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if len(getres.Kvs) == 0 {
		return nil, ErrNoKey
	}

	for _, str := range getres.Kvs {
		fetched = append(fetched, string(str.Value))
	}
	return fetched, nil
}

// Put sets the value for the given key
func (kvs *KVStore) Put(ctx context.Context, key, value string, options ...clientv3.OpOption) error {
	_, err := kvs.kv.Put(ctx, filepath.Join(storePrefix, key), value, options...)
	if err != nil {
		return fmt.Errorf("failed to put (key, value) (%s, %s): %w", key, value, err)
	}
	return nil
}

// Delete deletes the given key
func (kvs *KVStore) Delete(ctx context.Context, key string, options ...clientv3.OpOption) error {
	delres, err := kvs.kv.Delete(ctx, filepath.Join(storePrefix, key), options...)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	if delres.Deleted == 0 {
		return ErrNoKey
	}
	return nil
}
