package cluster

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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

	c, err := Join(ctx, cfg)
	require.NoError(t, err)
	defer c.Close()

	t.Run("test member list contains expected number of servers", func(t *testing.T) {
		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(members))
	})

	t.Run("test member add successfully adds new member", func(t *testing.T) {
		time.Sleep(etcdserver.HealthInterval)
        memberCfg, err := ConfigFromFile("./testdata/grep.yml")
        require.NoError(t, err)

		mai, err := c.MemberAdd(ctx, memberCfg.etcdConfig.LPUrls[0].String(), memberCfg.ServiceName, memberCfg.etcdConfig.Name, memberCfg.Port)
		require.NoError(t, err)

		services, err := c.Registry.Services(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, services["grep"])

		initialClusterStrings := []string{
			initialClusterStringFormatter(cfg.etcdConfig.Name, cfg.etcdConfig.LPUrls[0].String()),
			initialClusterStringFormatter(memberCfg.etcdConfig.Name, memberCfg.etcdConfig.LPUrls[0].String()),
		}
		sort.Strings(initialClusterStrings)
		initialCluster := fmt.Sprintf("%s,%s", initialClusterStrings[0], initialClusterStrings[1])

		expectedmai := &MemberAddInfo{
			InitialCluster:      initialCluster,
			ClusterState: "existing",
		}

		require.Equal(t, expectedmai, mai)

		memberCfg.etcdConfig.InitialCluster = mai.InitialCluster
		memberCfg.etcdConfig.ClusterState = mai.ClusterState

		e, err := startEmbeddedEtcd(memberCfg.etcdConfig)
		require.NoError(t, err)
		defer e.Close()

		members, err := c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, len(members))

		// Give time for servers to add node and wait for their health interval.
		time.Sleep(etcdserver.HealthInterval)

        LPUrl, err := url.Parse("http://127.0.0.1:32380")
		require.NoError(t, err)

        LCUrl, err := url.Parse("http://127.0.0.1:32379")
		require.NoError(t, err)

        APUrl, err := url.Parse("http://127.0.0.1:32380")
		require.NoError(t, err)

        ACUrl, err := url.Parse("http://127.0.0.1:32379")
		require.NoError(t, err)

        memberName := "node3"
		memberCfg = &Config{
            ServiceName: "testservice",
            Port: 8080,
            etcdConfig: embed.NewConfig(),
        }
		memberCfg.etcdConfig.Name = memberName
		memberCfg.etcdConfig.Dir = "tmp3"
		memberCfg.etcdConfig.LPUrls = []url.URL{*LPUrl}
		memberCfg.etcdConfig.LCUrls = []url.URL{*LCUrl}
		memberCfg.etcdConfig.APUrls = []url.URL{*APUrl}
		memberCfg.etcdConfig.ACUrls = []url.URL{*ACUrl}

		mai, err = c.MemberAdd(ctx, memberCfg.etcdConfig.LPUrls[0].String(), memberCfg.ServiceName, memberCfg.etcdConfig.Name, memberCfg.Port)
		require.NoError(t, err)

		initialClusterStrings = []string{
			initialClusterStringFormatter(cfg.etcdConfig.Name, cfg.etcdConfig.LPUrls[0].String()),
			initialClusterStringFormatter(memberName, LPUrl.String()),
		}
		sort.Strings(initialClusterStrings)
		initialCluster = fmt.Sprintf("%s,%s", initialClusterStrings[0], initialClusterStrings[1])

		expectedmai = &MemberAddInfo{
			InitialCluster:      initialCluster,
			ClusterState: "existing",
		}

		require.NotEqual(t, expectedmai, mai)

		memberCfg.etcdConfig.InitialCluster = mai.InitialCluster
		memberCfg.etcdConfig.ClusterState = mai.ClusterState

		e, err = startEmbeddedEtcd(memberCfg.etcdConfig)
		require.NoError(t, err)
		defer e.Close()

		members, err = c.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, len(members))
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
