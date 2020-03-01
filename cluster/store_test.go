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
	suite.SetupTest()
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
	_, err = tmpKV.Put(ctx, "store/raccoon1", expected)
	require.NoError(t, err)
	_, err = tmpKV.Put(ctx, "store/raccoon2", "uwu2")
	require.NoError(t, err)

	val, err := kvs.Get(ctx, "raccoon")
	require.NoError(t, err)
	require.Equal(t, expected, val, "value read back should be the same")
}

func (suite *EtcdDependentSuite) TestKVGetErrorsOnNoKey() {
	suite.SetupTest()
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	val, err := kvs.Get(ctx, "raccoon")
	require.Equal(t, ErrNoKey, err, "error returned should be ErrNoKey")
	require.Equal(t, val, "")
}

func (suite *EtcdDependentSuite) TestKVGetPrefix() {
	suite.SetupTest()
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
	_, err = tmpKV.Put(ctx, "store/raccoon1", "uwu1")
	require.NoError(t, err)
	_, err = tmpKV.Put(ctx, "store/raccoon2", "uwu2")
	require.NoError(t, err)

	val, err := kvs.GetPrefix(ctx, "raccoon")
	require.NoError(t, err)
	require.Equal(t, []string{"uwu1", "uwu2"}, val, "value read back should be the same")
}

func (suite *EtcdDependentSuite) TestKVGetPrefixErrorsOnNoKey() {
	suite.SetupTest()
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	val, err := kvs.GetPrefix(ctx, "raccoon")
	require.Equal(t, ErrNoKey, err, "error returned should be ErrNoKey")
	require.Nil(t, val)
}

func (suite *EtcdDependentSuite) TestKVPut() {
	suite.SetupTest()
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	expected := "world"
	err = kvs.Put(ctx, "hello", expected)
	require.NoError(t, err)

	val, err := kvs.Get(ctx, "hello")
	require.Equal(t, val, expected, "val returned should be expected")
	require.NoError(t, err)
}

func (suite *EtcdDependentSuite) TestKVDelete() {
	suite.SetupTest()
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	expected := "world"
	err = kvs.Put(ctx, "hello", expected)
	require.NoError(t, err)

	err = kvs.Delete(ctx, "hello")
	require.NoError(t, err)

	val, err := kvs.Get(ctx, "hello")
	require.Equal(t, err, ErrNoKey, "no key should be left after deletion")
	require.Equal(t, val, "")
}

func (suite *EtcdDependentSuite) TestKVDeleteNoKey() {
	suite.SetupTest()
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvs, err := NewKVStore(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	err = kvs.Delete(ctx, "hello")
	require.Equal(t, err, ErrNoKey, "no key to delete should yield ErrNoKey")
}
