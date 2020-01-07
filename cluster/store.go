package cluster

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client"
)

const storePrefix = "store"

type KVStore struct {
	kapi client.KeysAPI
}

func NewKVStore(ctx context.Context, etcdAddr string) (*KVStore, error) {
	cfg := client.Config{Endpoints: []string{etcdAddr}}
	c, err := client.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client from addr %v: %w", etcdAddr, err)
	}

	return &KVStore{
		kapi: client.NewKeysAPI(c),
	}, nil
}
