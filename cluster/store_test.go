package cluster

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

func TestNewKVStore(t *testing.T) {
	store, err := NewKVStore(context.Background(), "")
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestKVGet(t *testing.T) {
	cleanEtcdDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, TestEtcdAddr)
	require.NoError(t, err)

	// set up raw connection for test setup
	cfg := clientv3.Config{
		Endpoints:   []string{TestEtcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	require.NoError(t, err)
	defer c.Close()

	tmpKV := clientv3.NewKV(c)
	expected := "uwu"
	tmpKV.Put(ctx, "store/raccoon", expected)

	val, err := kvs.Get(ctx, "raccoon")
	require.NoError(t, err)
	require.Equal(t, expected, val, "value read back should be the same")
}
