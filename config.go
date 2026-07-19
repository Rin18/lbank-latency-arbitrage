package main

import (
	"os"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Symbols            []string
	LbankToken         string
	TelegramToken      string
	TelegramChatID     int64
	ArbitrageThreshold float64
	WsTimeOut          int64
	SlowWindowLengthms int64
	FastWindowLengthms int64
	Volume             int64
	IsIOC              bool
}

func ReadConfig(file string) (Config, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{}
	err = toml.Unmarshal(data, &cfg)
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}
