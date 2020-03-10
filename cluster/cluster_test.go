package cluster

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coreos/etcd/etcdserver"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/embed"
)

func TestClusterSuite(t *testing.T) {
	suite.Run(t, new(ClusterSuite))
}

type ClusterSuite struct {
	suite.Suite
}

func (suite *ClusterSuite) SetupTest() {
	cleanDir(suite.T())
}

func (suite *ClusterSuite) TestJoin() {
	t := suite.T()

	cfg, err := ConfigFromFile("./testdata/ping.yml")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO add more nodes to the test
	c, err := Join(ctx, cfg)
	require.NoError(t, err)
	defer c.Close()

	t.Run("test registry contains expected services", func(t *testing.T) {
		services, err := c.Registry.Services(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, services["ping"])
	})
}

func (suite *ClusterSuite) TestMemberAdd() {
	t := suite.T()

	cfg, err := ConfigFromFile("./testdata/ping.yml")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg.etcdConfig.Debug = true
	c, err := Join(ctx, cfg)
	require.NoError(t, err)
	defer c.Close()

	t.Run("test member list contains expected number of servers", func(t *testing.T) {
		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(members))
	})

	t.Run("test member add successfully adds new member", func(t *testing.T) {
		LPUrl, err := url.Parse("http://127.0.0.1:22380")
		require.NoError(t, err)

		LCUrl, err := url.Parse("http://127.0.0.1:22379")
		require.NoError(t, err)

		APUrl, err := url.Parse("http://127.0.0.1:22380")
		require.NoError(t, err)

		ACUrl, err := url.Parse("http://127.0.0.1:22379")
		require.NoError(t, err)

		memberName := "node2"
		memberCfg := embed.NewConfig()
		memberCfg.Name = memberName
		memberCfg.Dir = "tmp2"
		memberCfg.LPUrls = []url.URL{*LPUrl}
		memberCfg.LCUrls = []url.URL{*LCUrl}
		memberCfg.APUrls = []url.URL{*APUrl}
		memberCfg.ACUrls = []url.URL{*ACUrl}

		mai, err := c.MemberAdd(ctx, memberCfg.Name, memberCfg.LPUrls[0].String())
		require.NoError(t, err)

		memberCfg.InitialCluster = mai.InitialCluster
		memberCfg.ClusterState = mai.InitialClusterState

		fmt.Println(mai.InitialCluster)

		e, err := startEmbeddedEtcd(memberCfg)
		require.NoError(t, err)
		defer e.Close()

		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(members))

		// Give time for servers to add node and wait for their health interval.
		time.Sleep(etcdserver.HealthInterval)

		// Test

		LPUrl, err = url.Parse("http://127.0.0.1:32380")
		require.NoError(t, err)

		LCUrl, err = url.Parse("http://127.0.0.1:32379")
		require.NoError(t, err)

		APUrl, err = url.Parse("http://127.0.0.1:32380")
		require.NoError(t, err)

		ACUrl, err = url.Parse("http://127.0.0.1:32379")
		require.NoError(t, err)

		memberName = "node3"
		memberCfg = embed.NewConfig()
		memberCfg.Name = memberName
		memberCfg.Dir = "tmp3"
		memberCfg.LPUrls = []url.URL{*LPUrl}
		memberCfg.LCUrls = []url.URL{*LCUrl}
		memberCfg.APUrls = []url.URL{*APUrl}
		memberCfg.ACUrls = []url.URL{*ACUrl}

		mai, err = c.MemberAdd(ctx, memberCfg.Name, memberCfg.LPUrls[0].String())
		require.NoError(t, err)

		memberCfg.InitialCluster = mai.InitialCluster
		memberCfg.ClusterState = mai.InitialClusterState

		fmt.Println(mai.InitialCluster)

		e, err = startEmbeddedEtcd(memberCfg)
		require.NoError(t, err)
		defer e.Close()

		members, err = c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, len(members))

		e.Close()
		members, err = c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, len(members))

		// add again
		time.Sleep(etcdserver.HealthInterval + time.Second)

		LPUrl, err = url.Parse("http://127.0.0.1:42380")
		require.NoError(t, err)

		LCUrl, err = url.Parse("http://127.0.0.1:42379")
		require.NoError(t, err)

		APUrl, err = url.Parse("http://127.0.0.1:42380")
		require.NoError(t, err)

		ACUrl, err = url.Parse("http://127.0.0.1:42379")
		require.NoError(t, err)

		memberName = "node4"
		memberCfg = embed.NewConfig()
		memberCfg.Name = memberName
		memberCfg.Dir = "tmp4"
		memberCfg.LPUrls = []url.URL{*LPUrl}
		memberCfg.LCUrls = []url.URL{*LCUrl}
		memberCfg.APUrls = []url.URL{*APUrl}
		memberCfg.ACUrls = []url.URL{*ACUrl}

		mai, err = c.MemberAdd(ctx, memberCfg.Name, memberCfg.LPUrls[0].String())
		require.NoError(t, err)

		memberCfg.InitialCluster = mai.InitialCluster
		memberCfg.ClusterState = mai.InitialClusterState

		fmt.Println(mai.InitialCluster)

		e, err = startEmbeddedEtcd(memberCfg)
		require.NoError(t, err)
		defer e.Close()

		members, err = c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 4, len(members))
	})
}

func removeDirs(glob string) error {
	dirs, err := filepath.Glob(glob)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}

func cleanDir(t *testing.T) {
	err := removeDirs("tmp*")
	require.NoError(t, err)
}
