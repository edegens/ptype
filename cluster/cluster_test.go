package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	cfg, err := ConfigFromFile("./testdata/ping.yml")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO add more nodes to the test
	c, err := Join(ctx, cfg)
	require.NoError(t, err)

	t.Run("test registery contains expected services", func(t *testing.T) {
		services, err := c.Registry.Services(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, services["ping"])
	})
}
