package cluster

import (
	"context"
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

		memberCfg := Config{
			ServiceName:              "testservice",
			NodeName:                 "node2",
			Port:                     3030,
			InitialClusterClientUrls: []string{cfg.etcdConfig.LCUrls[0].String()},
			etcdConfig:               embed.NewConfig(),
		}
		memberCfg.etcdConfig.Name = "node2"
		memberCfg.etcdConfig.Dir = "tmp2"
		memberCfg.etcdConfig.Logger = "zap"
		memberCfg.etcdConfig.LPUrls = []url.URL{*LPUrl}
		memberCfg.etcdConfig.LCUrls = []url.URL{*LCUrl}
		memberCfg.etcdConfig.APUrls = []url.URL{*LPUrl}
		memberCfg.etcdConfig.ACUrls = []url.URL{*LCUrl}
		memberCfg.etcdConfig.ClusterState = embed.ClusterStateFlagExisting

		c2, err := Join(ctx, memberCfg)
		require.NoError(t, err)
		defer c2.Close()

		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(members))

		LPUrl, err = url.Parse("http://127.0.0.1:32380")
		require.NoError(t, err)

		LPUrl2, err := url.Parse("http://127.0.0.1:32480")
		require.NoError(t, err)

		LCUrl, err = url.Parse("http://127.0.0.1:32379")
		require.NoError(t, err)

		LCUrl2, err := url.Parse("http://127.0.0.1:32479")
		require.NoError(t, err)

		memberCfg = Config{
			ServiceName:              "testservice2",
			NodeName:                 "node3",
			Port:                     8080,
			InitialClusterClientUrls: []string{cfg.etcdConfig.LCUrls[0].String()},
			etcdConfig:               embed.NewConfig(),
		}
		memberCfg.etcdConfig.Name = "node3"
		memberCfg.etcdConfig.Dir = "tmp3"
		memberCfg.etcdConfig.Logger = "zap"
		memberCfg.etcdConfig.LPUrls = []url.URL{*LPUrl, *LPUrl2}
		memberCfg.etcdConfig.LCUrls = []url.URL{*LCUrl, *LCUrl2}
		memberCfg.etcdConfig.APUrls = []url.URL{*LPUrl, *LPUrl2}
		memberCfg.etcdConfig.ACUrls = []url.URL{*LCUrl, *LCUrl2}
		memberCfg.etcdConfig.ClusterState = embed.ClusterStateFlagExisting

		c3, err := Join(ctx, memberCfg)
		require.NoError(t, err)
		defer c3.Close()

		members, err = c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, len(members))

		t.Run("test add node with one faulty node in three node cluster", func(t *testing.T) {
			LPUrl, err := url.Parse("http://127.0.0.1:42380")
			require.NoError(t, err)

			LCUrl, err := url.Parse("http://127.0.0.1:42379")
			require.NoError(t, err)

			memberCfg := Config{
				ServiceName:              "testservice3",
				NodeName:                 "node4",
				Port:                     4040,
				InitialClusterClientUrls: []string{cfg.etcdConfig.LCUrls[0].String()},
				etcdConfig:               embed.NewConfig(),
			}
			memberCfg.etcdConfig.Name = "node4"
			memberCfg.etcdConfig.Dir = "tmp4"
			memberCfg.etcdConfig.Logger = "zap"
			memberCfg.etcdConfig.LPUrls = []url.URL{*LPUrl}
			memberCfg.etcdConfig.LCUrls = []url.URL{*LCUrl}
			memberCfg.etcdConfig.APUrls = []url.URL{*LPUrl}
			memberCfg.etcdConfig.ACUrls = []url.URL{*LCUrl}
			memberCfg.etcdConfig.ClusterState = embed.ClusterStateFlagExisting

			c3.Close()

			c4, err := Join(ctx, memberCfg)
			require.NoError(t, err)
			defer c4.Close()

			members, err := c.MemberList(ctx)
			require.NoError(t, err)
			require.Equal(t, 4, len(members))
		})
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
