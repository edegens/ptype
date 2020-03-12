package cluster

import (
	"go.etcd.io/etcd/clientv3"
)

type SortTarget int

const (
	SortByKey SortTarget = iota
	SortByVersion
	SortByCreateRevision
	SortByModRevision
	SortByValue
)

var (
	noPrefixEnd = []byte{0}
)

type SortOrder int

const (
	SortNone SortOrder = iota
	SortAscend
	SortDescend
)

// WithSort specifies the ordering in 'Get' request. It requires
// 'WithRange' and/or 'WithPrefix' to be specified too.
// 'target' specifies the target to sort by: key, version, revisions, value.
// 'order' can be either 'SortNone', 'SortAscend', 'SortDescend'.
func WithSort(target SortTarget, order SortOrder) clientv3.OpOption {
	targetVal := clientv3.SortTarget(target)
	orderVal := clientv3.SortOrder(order)
	return clientv3.WithSort(targetVal, orderVal)
}

// GetPrefixRangeEnd gets the range end of the prefix.
// 'Get(foo, WithPrefix())' is equal to 'Get(foo, WithRange(GetPrefixRangeEnd(foo))'.
func GetPrefixRangeEnd(prefix string) string {
	return string(getPrefix([]byte(prefix)))
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	return noPrefixEnd
}

// WithPrefix enables 'Get', 'Delete', or 'Watch' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() clientv3.OpOption {
	return clientv3.WithPrefix()
}

// WithLimit limits the number of results to return from 'Get' request.
// If WithLimit is given a 0 limit, it is treated as no limit.
func WithLimit(n int64) clientv3.OpOption { return clientv3.WithLimit(n) }

// WithRev specifies the store revision for 'Get' request.
// Or the start revision of 'Watch' request.
func WithRev(rev int64) clientv3.OpOption { return clientv3.WithRev(rev) }

// WithRange specifies the range of 'Get', 'Delete', 'Watch' requests.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
// endKey must be lexicographically greater than start key.
func WithRange(endKey string) clientv3.OpOption {
	return clientv3.WithRange(endKey)
}

// WithFromKey specifies the range of 'Get', 'Delete', 'Watch' requests
// to be equal or greater than the key in the argument.
func WithFromKey() clientv3.OpOption { return clientv3.WithRange("\x00") }

// WithSerializable makes 'Get' request serializable. By default,
// it's linearizable. Serializable requests are better for lower latency
// requirement.
func WithSerializable() clientv3.OpOption {
	return clientv3.WithSerializable()
}

// WithKeysOnly makes the 'Get' request return only the keys and the corresponding
// values will be omitted.
func WithKeysOnly() clientv3.OpOption {
	return clientv3.WithKeysOnly()
}

// WithCountOnly makes the 'Get' request return only the count of keys.
func WithCountOnly() clientv3.OpOption {
	return clientv3.WithCountOnly()
}

// WithMinModRev filters out keys for Get with modification revisions less than the given revision.
func WithMinModRev(rev int64) clientv3.OpOption { return clientv3.WithMinModRev(rev) }

// WithMaxModRev filters out keys for Get with modification revisions greater than the given revision.
func WithMaxModRev(rev int64) clientv3.OpOption { return clientv3.WithMaxModRev(rev) }

// WithMinCreateRev filters out keys for Get with creation revisions less than the given revision.
func WithMinCreateRev(rev int64) clientv3.OpOption { return clientv3.WithMinCreateRev(rev) }

// WithMaxCreateRev filters out keys for Get with creation revisions greater than the given revision.
func WithMaxCreateRev(rev int64) clientv3.OpOption { return clientv3.WithMaxCreateRev(rev) }

// WithFirstCreate gets the key with the oldest creation revision in the request range.
func WithFirstCreate() []clientv3.OpOption { return withTop(SortByCreateRevision, SortAscend) }

// WithLastCreate gets the key with the latest creation revision in the request range.
func WithLastCreate() []clientv3.OpOption { return withTop(SortByCreateRevision, SortDescend) }

// WithFirstKey gets the lexically first key in the request range.
func WithFirstKey() []clientv3.OpOption { return withTop(SortByKey, SortAscend) }

// WithLastKey gets the lexically last key in the request range.
func WithLastKey() []clientv3.OpOption { return withTop(SortByKey, SortDescend) }

// WithFirstRev gets the key with the oldest modification revision in the request range.
func WithFirstRev() []clientv3.OpOption { return withTop(SortByModRevision, SortAscend) }

// WithLastRev gets the key with the latest modification revision in the request range.
func WithLastRev() []clientv3.OpOption { return withTop(SortByModRevision, SortDescend) }

// withTop gets the first key over the get's prefix given a sort order
func withTop(target SortTarget, order SortOrder) []clientv3.OpOption {
	targetVal := clientv3.SortTarget(target)
	orderVal := clientv3.SortOrder(order)
	return []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithSort(targetVal, orderVal), clientv3.WithLimit(1)}
}

// WithProgressNotify makes watch server send periodic progress updates
// every 10 minutes when there is no incoming events.
// Progress updates have zero events in WatchResponse.
func WithProgressNotify() clientv3.OpOption {
	return clientv3.WithProgressNotify()
}

// WithCreatedNotify makes watch server sends the created event.
func WithCreatedNotify() clientv3.OpOption {
	return clientv3.WithCreatedNotify()
}

// WithFilterPut discards PUT events from the watcher.
func WithFilterPut() clientv3.OpOption {
	return clientv3.WithFilterPut()
}

// WithFilterDelete discards DELETE events from the watcher.
func WithFilterDelete() clientv3.OpOption {
	return clientv3.WithFilterDelete()
}

// WithPrevKV gets the previous key-value pair before the event happens. If the previous KV is already compacted,
// nothing will be returned.
func WithPrevKV() clientv3.OpOption {
	return clientv3.WithPrevKV()
}

// WithIgnoreValue updates the key using its current value.
// This option can not be combined with non-empty values.
// Returns an error if the key does not exist.
func WithIgnoreValue() clientv3.OpOption {
	return clientv3.WithIgnoreValue()
}

// WithIgnoreLease updates the key using its current lease.
// This option can not be combined with WithLease.
// Returns an error if the key does not exist.
func WithIgnoreLease() clientv3.OpOption {
	return clientv3.WithIgnoreLease()
}
