package cluster

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

var TestEtcdAddr string

func TestMain(m *testing.M) {
	addr, cleanup := startTestEtcd()
	defer cleanup()
	TestEtcdAddr = addr

	os.Exit(m.Run())
}

func TestServiceRegistry_Register(t *testing.T) {
	cleanEtcdDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sr, err := NewServiceRegistry(ctx, TestEtcdAddr)
	require.NoError(t, err)

	err = sr.Register(context.Background(), "foo", "node1", "host", 8000)
	require.NoError(t, err)
	err = sr.Register(context.Background(), "foo", "node2", "host2", 8000)
	require.NoError(t, err)
	err = sr.Register(context.Background(), "bar", "node3", "host3", 3000)
	require.NoError(t, err)

	t.Run("test multiple nodes registered for foo", func(t *testing.T) {
		key := filepath.Join(servicesPrefix, "foo")
		res, err := sr.KV.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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
		res, err := sr.KV.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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

func TestServiceRegistry_Services(t *testing.T) {
	cleanEtcdDir(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sr, err := NewServiceRegistry(ctx, TestEtcdAddr)
	require.NoError(t, err)

	key := filepath.Join(servicesPrefix, "foo", "node1")
	_, err = sr.KV.Put(ctx, key, `{"address":"host", "port":8000}`)
	require.NoError(t, err)
	key = filepath.Join(servicesPrefix, "foo", "node2")
	_, err = sr.KV.Put(ctx, key, `{"address":"host2", "port":8000}`)
	require.NoError(t, err)
	key = filepath.Join(servicesPrefix, "bar", "node3")
	_, err = sr.KV.Put(ctx, key, `{"address":"host3", "port":3000}`)
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

func cleanEtcdDir(t *testing.T) {
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{TestEtcdAddr}})
	require.NoError(t, err)
	KV := clientv3.NewKV(c)
	// wipe services dir for every test
	_, err = KV.Delete(context.Background(), servicesPrefix, clientv3.WithPrefix())
    if err != nil {
        return
    }

	require.NoError(t, err)
}
