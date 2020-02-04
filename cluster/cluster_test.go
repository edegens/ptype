package cluster

import (
	"context"
    "fmt"
	"testing"

	"github.com/stretchr/testify/require"
    "go.etcd.io/etcd/embed"
)

func TestJoin(t *testing.T) {
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

func TestMemberList(t *testing.T) {
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
}

func TestMemberAdd(t *testing.T) {
    cfg, err := ConfigFromFile("./testdata/ping.yml")
    require.NoError(t, err)

    cfg2, err := ConfigFromFile("./testdata/foo.yml")
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    c, err := Join(ctx, cfg)
    require.NoError(t, err)
    defer c.Close()

    t.Run("test member add successfully adds new member", func(t *testing.T) {
        lcfg, err := embed.ConfigFromFile(fmt.Sprintf("./testdata/%s", cfg.EtcdConfigFile))
        require.NoError(t, err)

        mcfg, err := embed.ConfigFromFile(fmt.Sprintf("./testdata/%s", cfg2.EtcdConfigFile))
        require.NoError(t, err)

        members, err := c.MemberList(ctx)
        require.NoError(t, err)
        require.Equal(t, 1, len(members))

        mai, err := c.MemberAdd(ctx, mcfg.LCUrls[0].String())
        require.NoError(t, err)

        expectedmai := &MemberAddInfo{
            Name: lcfg.Name,
            InitialCluster: lcfg.InitialCluster,
            InitialClusterState: lcfg.ClusterState,
        }

        require.Equal(t, expectedmai, mai)

        e, err := embed.StartEtcd(mcfg)
        require.NoError(t, err)
        defer e.Close()

        members, err = c.MemberList(ctx)
        require.NoError(t, err)
        require.Equal(t, 2, len(members))
    })
}
