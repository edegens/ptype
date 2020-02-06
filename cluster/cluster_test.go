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
		lcfg, err := embed.ConfigFromFile(fmt.Sprintf("./testdata/%s", cfg.EtcdConfigFile))
		require.NoError(t, err)

		LPUrl, err := url.Parse("http://127.0.0.1:22380")
		require.NoError(t, err)

		LCUrl, err := url.Parse("http://127.0.0.1:22379")
		require.NoError(t, err)

		APUrl, err := url.Parse("http://127.0.0.1:22380")
		require.NoError(t, err)

		ACUrl, err := url.Parse("http://127.0.0.1:22379")
		require.NoError(t, err)

		mecfg := &EtcdConfig{
			Name:           "node2",
			DataDir:        "tmp2",
			LPUrls:         []url.URL{*LPUrl},
			LCUrls:         []url.URL{*LCUrl},
			APUrls:         []url.URL{*APUrl},
			ACUrls:         []url.URL{*ACUrl},
			InitialCluster: "node2=http://127.0.0.1:22380",
		}

		mcfg := embed.NewConfig()
		mcfg.Name = mecfg.Name
		mcfg.Dir = mecfg.DataDir
		mcfg.LPUrls = mecfg.LPUrls
		mcfg.APUrls = mecfg.APUrls
		mcfg.ACUrls = mecfg.ACUrls

		mai, err := c.MemberAdd(ctx, mcfg.LPUrls[0].String())
		require.NoError(t, err)

		expectedmai := &MemberAddInfo{
			InitialCluster:      lcfg.InitialCluster,
			InitialClusterState: "existing",
		}

		require.Equal(t, expectedmai, mai)

		mcfg.InitialCluster = fmt.Sprintf("%s,%s", mai.InitialCluster, mecfg.InitialCluster)
		mcfg.ClusterState = mai.InitialClusterState

		e, err := embed.StartEtcd(mcfg)
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
