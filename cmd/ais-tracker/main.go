package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vessel-ais-tracker/config"
	"github.com/vessel-ais-tracker/internal/listener"
	"github.com/vessel-ais-tracker/internal/model"
	"github.com/vessel-ais-tracker/internal/parser"
	"github.com/vessel-ais-tracker/internal/pipeline"
	"github.com/vessel-ais-tracker/internal/storage"
)

func main() {
	cfg := config.Default()

	rawMsgChan := make(chan *model.AISMessage, 10000)
	parsedMsgChan := make(chan *model.ParsedMessage, 10000)
	batchChan := make(chan []*model.VesselData, 100)

	lis := listener.New(cfg.Listener, rawMsgChan)
	par := parser.New(cfg.Parser, rawMsgChan, parsedMsgChan)
	pipe := pipeline.New(cfg.Pipeline, parsedMsgChan, batchChan)
	store := storage.New(cfg.Storage, batchChan)

	log.Println("Starting AIS Tracker Service...")

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

		log.Printf("STATS - Listener: pkts=%d bytes=%d errs=%d | "+
			"Parser: parsed=%d failed=%d | "+
			"Pipeline: %s | "+
			"Storage: batches=%d records=%d failed=%d workers=%d pending=%d circuit=%s",
			lisStats.PacketsReceived, lisStats.BytesReceived, lisStats.Errors,
			parStats.MessagesParsed, parStats.MessagesFailed,
			pipeStats.String(),
			storeStats.BatchesWritten, storeStats.RecordsWritten, storeStats.BatchesFailed,
			storeStats.ActiveWorkers, storeStats.PendingBatches, storeStats.CircuitState)
	}
}
