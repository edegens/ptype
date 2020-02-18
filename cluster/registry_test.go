package cluster

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestEtcdDependentSuite(t *testing.T) {
	suite.Run(t, new(EtcdDependentSuite))
}

type EtcdDependentSuite struct {
	suite.Suite
	testEtcdAddr string
	cleanup      func()
}

func (suite *EtcdDependentSuite) SetupSuite() {
	addr, cleanup := startTestEtcd()
	suite.cleanup = cleanup
	suite.testEtcdAddr = addr
}

func (suite *EtcdDependentSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *EtcdDependentSuite) SetupTest() {
	cleanEtcdDir(suite.T(), suite.testEtcdAddr)
}

func (suite *EtcdDependentSuite) TestEtcdRegistry_Register() {
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sr, err := newEtcdRegistry(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	err = sr.Register(context.Background(), "foo", "node1", "host", 8000)
	require.NoError(t, err)
	err = sr.Register(context.Background(), "foo", "node2", "host2", 8000)
	require.NoError(t, err)
	err = sr.Register(context.Background(), "bar", "node3", "host3", 3000)
	require.NoError(t, err)

	t.Run("test multiple nodes registered for foo", func(t *testing.T) {
		key := filepath.Join(servicesPrefix, "foo")
		res, err := sr.kv.Get(ctx, key, defaultGetOptions...)
		require.NoError(t, err)

		require.Len(t, res.Kvs, 2)

		expected := []string{
			`{"address":"host", "port":8000}`,
			`{"address":"host2", "port":8000}`,
		}
		for i, Kvs := range res.Kvs {
			require.JSONEq(t, expected[i], string(Kvs.Value))
		}
	})

	t.Run("test one node registered for bar", func(t *testing.T) {
		key := filepath.Join(servicesPrefix, "bar")
		res, err := sr.kv.Get(ctx, key, defaultGetOptions...)
		require.NoError(t, err)

		require.Len(t, res.Kvs, 1)

		expected := []string{
			`{"address":"host3", "port":3000}`,
		}

		for i, Kvs := range res.Kvs {
			require.JSONEq(t, expected[i], string(Kvs.Value))
		}
	})
}

func (suite *EtcdDependentSuite) TestEtcdRegistry_Services() {
	t := suite.T()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sr, err := newEtcdRegistry(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	key := filepath.Join(servicesPrefix, "foo", "node1")
	_, err = sr.kv.Put(ctx, key, `{"address":"host", "port":8000}`)
	require.NoError(t, err)
	key = filepath.Join(servicesPrefix, "foo", "node2")
	_, err = sr.kv.Put(ctx, key, `{"address":"host2", "port":8000}`)
	require.NoError(t, err)
	key = filepath.Join(servicesPrefix, "bar", "node3")
	_, err = sr.kv.Put(ctx, key, `{"address":"host3", "port":3000}`)
	require.NoError(t, err)

	actual, err := sr.Services(ctx)
	require.NoError(t, err)

	expected := map[string][]Node{
		"foo": {
			{Address: "host", Port: 8000},
			{Address: "host2", Port: 8000},
		},
		"bar": {
			{Address: "host3", Port: 3000},
		},
	}
	require.Equal(t, expected, actual)
}

func (suite *EtcdDependentSuite) TestServiceRegistry_Leases() {
	t := suite.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sr, err := newEtcdRegistry(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	err = sr.Register(ctx, "bar", "node1", "host", 8000)
	require.NoError(t, err)

	t.Run("test lease expires when context is canceled", func(t *testing.T) {
		cancel()
		time.Sleep(time.Second * 5)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		key := filepath.Join(servicesPrefix, "bar")
		res, err := sr.kv.Get(ctx, key, defaultGetOptions...)
		require.NoError(t, err)

		require.Len(t, res.Kvs, 0)
	})
}

func (suite *EtcdDependentSuite) TestEtcdRegistry_WatchService() {
	t := suite.T()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sr, err := newEtcdRegistry(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	key := filepath.Join(servicesPrefix, "foo", "node1")
	_, err = sr.kv.Put(ctx, key, `{"address":"host", "port":8000}`)
	require.NoError(t, err)

	nodesChan := sr.WatchService(ctx, "foo")

	t.Run("channel returns the inital list of nodes before channel is created", func(t *testing.T) {
		require.Equal(t, []Node{
			{Address: "host", Port: 8000},
		}, <-nodesChan)
	})

	t.Run("channel returns all nodes on event change", func(t *testing.T) {
		key = filepath.Join(servicesPrefix, "foo", "node3")
		_, err = sr.kv.Put(ctx, key, `{"address":"host3", "port":3000}`)
		require.NoError(t, err)

		require.Equal(t, []Node{
			{Address: "host", Port: 8000},
			{Address: "host3", Port: 3000},
		}, <-nodesChan)
	})

	t.Run("handles the deletion of a node", func(t *testing.T) {
		key = filepath.Join(servicesPrefix, "foo", "node1")
		_, err = sr.kv.Delete(ctx, key)
		require.NoError(t, err)

		require.Equal(t, []Node{
			{Address: "host3", Port: 3000},
		}, <-nodesChan)
	})

}

func (suite *EtcdDependentSuite) TestEtcdRegistry_WatchService_stops_with_context_cancel() {
	t := suite.T()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sr, err := newEtcdRegistry(ctx, suite.testEtcdAddr)
	require.NoError(t, err)

	nodesChan := sr.WatchService(ctx, "foo")
	cancel()

	_, ok := <-nodesChan
	require.False(t, ok)
}

func startTestEtcd() (string, func()) {
	cfg := embed.NewConfig()

	tmp, err := ioutil.TempDir("", "test_etcd")
	cfg.Dir = tmp
	if err != nil {
		log.Fatal(err)
	}

	e, err := startEmbeddedEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}

	addr := cfg.LCUrls[0].String()
	return addr, func() {
		e.Close()
		<-e.Server.StopNotify()
		os.RemoveAll(tmp)
	}
}

func cleanEtcdDir(t *testing.T, testEtcdAddr string) {
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{testEtcdAddr}})
	require.NoError(t, err)
	KV := clientv3.NewKV(c)
	// wipe services dir for every test
	_, err = KV.Delete(context.Background(), servicesPrefix, clientv3.WithPrefix())
	if err != nil {
		return
	}

	require.NoError(t, err)
}
