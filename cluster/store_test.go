package cluster

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewKVStore(t *testing.T) {
	store, err := NewKVStore(context.Background(), "")
	require.NoError(t, err)
	require.NotNil(t, store)
}
