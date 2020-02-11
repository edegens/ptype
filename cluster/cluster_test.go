package cluster

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

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

		membercfg := embed.NewConfig()
		membercfg.Name = "node2"
		membercfg.Dir = "tmp2"
		membercfg.LPUrls = []url.URL{*LPUrl}
		membercfg.LCUrls = []url.URL{*LCUrl}
		membercfg.APUrls = []url.URL{*APUrl}
		membercfg.ACUrls = []url.URL{*ACUrl}

		mai, err := c.MemberAdd(ctx, membercfg.LPUrls[0].String())
		require.NoError(t, err)

		expectedmai := &MemberAddInfo{
			InitialCluster:      cfg.etcdConfig.InitialCluster,
			InitialClusterState: "existing",
		}

		require.Equal(t, expectedmai, mai)

		membercfg.InitialCluster = fmt.Sprintf("%s,%s=%s", mai.InitialCluster, membercfg.Name, membercfg.LPUrls[0].String())
		membercfg.ClusterState = mai.InitialClusterState

		e, err := embed.StartEtcd(membercfg)
		require.NoError(t, err)
		defer e.Close()

		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(members))
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
