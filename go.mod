module github.com/edegens/ptype

go 1.13

require (
	github.com/stretchr/testify v1.4.0
	// hack to get version v3.4.4, since upstream go.mod file is broken
	go.etcd.io/etcd v0.0.0-20200224211402-c65a9e2dd1fd
	go.uber.org/zap v1.12.0
	sigs.k8s.io/yaml v1.1.0
)
