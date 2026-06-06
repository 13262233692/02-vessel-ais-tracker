package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/vessel-ais-tracker/config"
	"github.com/vessel-ais-tracker/internal/geofence"
	"github.com/vessel-ais-tracker/internal/listener"
	"github.com/vessel-ais-tracker/internal/model"
	"github.com/vessel-ais-tracker/internal/parser"
	"github.com/vessel-ais-tracker/internal/pipeline"
	"github.com/vessel-ais-tracker/internal/storage"
	"github.com/vessel-ais-tracker/internal/warning"
)

const (
	maxGoroutinesThreshold = 5000
)

func main() {
	cfg := config.Default()

	rawMsgChan := make(chan *model.AISMessage, 50000)
	parsedMsgChan := make(chan *model.ParsedMessage, 50000)
	batchChan := make(chan []*model.VesselData, 200)

	lis := listener.New(cfg.Listener, rawMsgChan)
	par := parser.New(cfg.Parser, rawMsgChan, parsedMsgChan)
	pipe := pipeline.New(cfg.Pipeline, parsedMsgChan, batchChan)
	store := storage.New(cfg.Storage, batchChan)

	log.Println("Starting AIS Tracker Service...")
	log.Printf("Max goroutine threshold: %d", maxGoroutinesThreshold)

	if err := lis.Start(); err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	log.Printf("Listener started on UDP port %d", cfg.Listener.UDPPort)

	par.Start()
	log.Printf("Parser started with %d workers", cfg.Parser.WorkerCount)

	pipe.Start()
	log.Printf("Pipeline started with ring buffer size %d, batch size %d",
		cfg.Pipeline.RingBufferSize, cfg.Pipeline.BatchSize)

	if err := store.Connect(); err != nil {
		log.Printf("Storage connect warning: %v (continuing without storage)", err)
	} else {
		if err := store.InitSchema(); err != nil {
			log.Printf("Schema init warning: %v", err)
		}
		store.Start()
		log.Println("Storage started")
	}

	var warnPipe *warning.WarningPipeline
	var logPublisher *warning.LogPublisher
	var redisPublisher *warning.RedisPublisher

	if cfg.Warning.Enabled {
		fenceMgr := geofence.NewManager()
		warning.LoadSampleFences(fenceMgr)
		log.Printf("Geofence manager loaded with %d zones", fenceMgr.FenceCount())

		warningInput := warning.TapInto(parsedMsgChan, cfg.Warning.InputQueueSize)

		warnPipe = warning.New(warning.Config{
			WorkerCount:    cfg.Warning.WorkerCount,
			InputQueueSize: cfg.Warning.InputQueueSize,
			AlertQueueSize: cfg.Warning.AlertQueueSize,
		}, warningInput, fenceMgr)
		warnPipe.Start()
		log.Printf("Warning pipeline started with %d workers", cfg.Warning.WorkerCount)

		logPublisher = warning.NewLogPublisher(warnPipe.AlertChannel())
		logPublisher.Start()
		log.Println("Alert log publisher started")

		_ = redisPublisher
	}

	go statsReporter(lis, par, pipe, store, warnPipe)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\nShutting down...")

	lis.Stop()
	par.Stop()
	pipe.Stop()
	store.Stop()

	if warnPipe != nil {
		warnPipe.Stop()
	}
	if logPublisher != nil {
		logPublisher.Stop()
	}

	log.Println("Shutdown complete")
}

func statsReporter(
	lis *listener.Listener,
	par *parser.Parser,
	pipe *pipeline.Pipeline,
	store *storage.Storage,
	warn *warning.WarningPipeline,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		lisStats := lis.GetStats()
		parStats := par.GetStats()
		pipeStats := pipe.GetStats()
		storeStats := store.GetStats()
		goroutines := runtime.NumGoroutine()

		goroutineWarn := ""
		if goroutines > maxGoroutinesThreshold {
			goroutineWarn = " ⚠️ HIGH GOROUTINE COUNT!"
		}

		warnStatsStr := ""
		if warn != nil {
			warnStats := warn.GetStats()
			warnStatsStr = " | Warning: " + warnStats.String()
		}

		log.Printf("STATS [goroutines=%d%s] | "+
			"Listener: pkts=%d bytes=%d errs=%d | "+
			"Parser: parsed=%d failed=%d dropped=%d | "+
			"Pipeline: %s | "+
			"Storage: written=%d records=%d failed=%d dropped=%d writeQ=%d cb=%s%s",
			goroutines, goroutineWarn,
			lisStats.PacketsReceived, lisStats.BytesReceived, lisStats.Errors,
			parStats.MessagesParsed, parStats.MessagesFailed, parStats.MessagesDropped,
			pipeStats.String(),
			storeStats.BatchesWritten, storeStats.RecordsWritten, storeStats.BatchesFailed,
			storeStats.BatchesDropped, storeStats.WriteQueueDepth, storeStats.CircuitState,
			warnStatsStr)
	}
}
