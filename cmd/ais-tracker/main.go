package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/vessel-ais-tracker/config"
	"github.com/vessel-ais-tracker/internal/listener"
	"github.com/vessel-ais-tracker/internal/model"
	"github.com/vessel-ais-tracker/internal/parser"
	"github.com/vessel-ais-tracker/internal/pipeline"
	"github.com/vessel-ais-tracker/internal/storage"
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

	go statsReporter(lis, par, pipe, store)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\nShutting down...")

	lis.Stop()
	par.Stop()
	pipe.Stop()
	store.Stop()

	log.Println("Shutdown complete")
}

func statsReporter(lis *listener.Listener, par *parser.Parser, pipe *pipeline.Pipeline, store *storage.Storage) {
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

		log.Printf("STATS [goroutines=%d%s] | "+
			"Listener: pkts=%d bytes=%d errs=%d | "+
			"Parser: parsed=%d failed=%d dropped=%d | "+
			"Pipeline: %s | "+
			"Storage: written=%d records=%d failed=%d dropped=%d writeQ=%d cb=%s",
			goroutines, goroutineWarn,
			lisStats.PacketsReceived, lisStats.BytesReceived, lisStats.Errors,
			parStats.MessagesParsed, parStats.MessagesFailed, parStats.MessagesDropped,
			pipeStats.String(),
			storeStats.BatchesWritten, storeStats.RecordsWritten, storeStats.BatchesFailed,
			storeStats.BatchesDropped, storeStats.WriteQueueDepth, storeStats.CircuitState)
	}
}
