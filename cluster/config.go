package cluster

import (
	"fmt"
	"io/ioutil"

	"go.etcd.io/etcd/embed"
	"sigs.k8s.io/yaml"
)

type Config struct {
	ServiceName    string `json:"service_name"`
	NodeName       string `json:"node_name"`
	Port           int    `json:"port"`
	EtcdConfigPath string `json:"etcd_config_path"`

	etcdConfig *embed.Config
}

func ConfigFromFile(cfgPath string) (Config, error) {
	var cfg Config

	cfgBytes, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return cfg, fmt.Errorf("failed to read cluster config at %v: %w", cfgPath, err)
	}

	if err := yaml.Unmarshal(cfgBytes, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to read yaml of cluster config: %w", err)
	}

	if cfg.etcdConfig, err = embed.ConfigFromFile(cfg.EtcdConfigPath); err != nil {
		return cfg, fmt.Errorf("failed to read etcd config from %v: %w", cfg.EtcdConfigPath, err)
	}

	if err := cfg.etcdConfig.Validate(); err != nil {
		return cfg, fmt.Errorf("etcd config provided is not valid: %w", err)
	}

	return cfg, nil
}
