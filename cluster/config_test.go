package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigFromFile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		path      string
		expected  Config
		expectErr bool
	}{
		{
			name: "simple config",
			path: "testdata/ping.yml",
			expected: Config{
				ServiceName:    "ping",
				NodeName:       "node1",
				Port:           3000,
				EtcdConfigFile: "node1.yml",
				Debug:          true,
			},
		},
		{
			name:      "bad config",
			path:      "testdata/bad_config.yml",
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ConfigFromFile(tc.path)
			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NotNil(t, cfg.etcdConfig)
			cfg.etcdConfig = nil // not our responsibility to test
			require.Equal(t, tc.expected, cfg)
		})
	}
}
