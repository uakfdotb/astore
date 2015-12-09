package main

import "code.google.com/p/gcfg"

import "log"

type ConfigDefault struct {
	Debug bool
}

type ConfigServers struct {
	Servers []string
}

type ConfigSSH struct {
	PrivateKeyPath string
}

type Config struct {
	Default ConfigDefault
	Servers ConfigServers
	SSH ConfigSSH
}

func LoadConfig(cfgPath string) *Config {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, cfgPath)
	if err != nil {
		log.Fatalf("Error while reading configuration: %s", err.Error())
	}
	return &cfg
}
