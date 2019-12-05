package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJoin_registers_all_nodes(t *testing.T) {
	cfg, err := ConfigFromFile("./testdata/ping.yml")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO add more nodes to the test
	c, err := Join(ctx, cfg)
	require.NoError(t, err)

	t.Run("test registery contains expected services", func(t *testing.T) {
		expected := []string{
			"/services/ping",
		}
		require.Equal(t, expected, c.Registry.Services())
	})

	// TODO
	t.Run("test registery contains expected nodes per service", func(t *testing.T) {
	})
}
