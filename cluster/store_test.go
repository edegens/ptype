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

func (suite *EtcdDependentSuite) TestKVGet() {
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	// set up raw connection for test setup
	cfg := clientv3.Config{
		Endpoints:   []string{suite.testEtcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	require.NoError(t, err)
	defer c.Close()

	tmpKV := clientv3.NewKV(c)
	expected := "uwu1"
	tmpKV.Put(ctx, "store/raccoon1", expected)
	tmpKV.Put(ctx, "store/raccoon2", "uwu2")

	val, err := kvs.Get(ctx, "raccoon")
	require.NoError(t, err)
	require.Equal(t, expected, val, "value read back should be the same")
}

func (suite *EtcdDependentSuite) TestKVGetAll() {
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	// set up raw connection for test setup
	cfg := clientv3.Config{
		Endpoints:   []string{suite.testEtcdAddr},
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	require.NoError(t, err)
	defer c.Close()

	tmpKV := clientv3.NewKV(c)
	tmpKV.Put(ctx, "store/raccoon1", "uwu1")
	tmpKV.Put(ctx, "store/raccoon2", "uwu2")

	val, err := kvs.GetAll(ctx, "raccoon")
	require.NoError(t, err)
	require.Equal(t, []string{"uwu1", "uwu2"}, val, "value read back should be the same")
}
