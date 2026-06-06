package config

import (
	"time"

	"github.com/vessel-ais-tracker/internal/listener"
	"github.com/vessel-ais-tracker/internal/parser"
	"github.com/vessel-ais-tracker/internal/pipeline"
	"github.com/vessel-ais-tracker/internal/storage"
)

type WarningConfig struct {
	Enabled        bool
	WorkerCount    int
	InputQueueSize int
	AlertQueueSize int
	RedisAddress   string
	RedisPassword  string
	RedisDB        int
	RedisChannel   string
}

type Config struct {
	Listener listener.Config
	Parser   parser.Config
	Pipeline pipeline.Config
	Storage  storage.Config
	Warning  WarningConfig
}

func Default() Config {
	return Config{
		Listener: listener.Config{
			UDPPort:       5005,
			ReadBuffer:    1024 * 1024 * 4,
			MaxPacketSize: 65535,
		},
		Parser: parser.Config{
			WorkerCount: 8,
		},
		Pipeline: pipeline.Config{
			RingBufferSize: 100000,
			BatchSize:      1000,
			FlushInterval:  500 * time.Millisecond,
			EnqueueWorkers: 2,
		},
		Storage: storage.Config{
			Host:            "localhost",
			Port:            5432,
			User:            "postgres",
			Password:        "postgres",
			DBName:          "ais",
			SSLMode:         "disable",
			MaxOpenConns:    20,
			MaxIdleConns:    10,
			ConnMaxLifetime: 10 * time.Minute,
			RetryCount:      3,
			RetryWait:       100 * time.Millisecond,
			MaxWriteWorkers: 4,
			WriteQueueSize:  200,
			WriteTimeout:    5 * time.Second,
		},
		Warning: WarningConfig{
			Enabled:        true,
			WorkerCount:    4,
			InputQueueSize: 50000,
			AlertQueueSize: 1000,
			RedisAddress:   "localhost:6379",
			RedisPassword:  "",
			RedisDB:        0,
			RedisChannel:   "ais:alerts:critical",
		},
	}
}
