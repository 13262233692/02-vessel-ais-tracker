package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/vessel-ais-tracker/internal/concurrency"
	"github.com/vessel-ais-tracker/internal/model"
)

type Config struct {
	Host              string
	Port              int
	User              string
	Password          string
	DBName            string
	SSLMode           string
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	RetryCount        int
	RetryWait         time.Duration
	MaxWriteWorkers   int
	WriteQueueSize    int
	WriteTimeout      time.Duration
}

type Storage struct {
	config            Config
	db                *sql.DB
	input             <-chan []*model.VesselData
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	batchesWritten    uint64
	recordsWritten    uint64
	batchesFailed     uint64
	batchesDropped    uint64
	lastWriteTime     int64
	lastWriteDuration int64
	writeCh           chan []*model.VesselData
	cb                *concurrency.CircuitBreaker
}

type StorageStats struct {
	BatchesWritten      uint64
	RecordsWritten      uint64
	BatchesFailed       uint64
	BatchesDropped      uint64
	LastWriteTime       time.Time
	LastWriteDuration   time.Duration
	WriteQueueDepth     int
	ActiveWorkers       int
	CircuitState        string
}

func New(config Config, input <-chan []*model.VesselData) *Storage {
	ctx, cancel := context.WithCancel(context.Background())

	if config.MaxWriteWorkers <= 0 {
		config.MaxWriteWorkers = 4
	}
	if config.WriteQueueSize <= 0 {
		config.WriteQueueSize = 200
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 5 * time.Second
	}

	return &Storage{
		config:  config,
		input:   input,
		ctx:     ctx,
		cancel:  cancel,
		writeCh: make(chan []*model.VesselData, config.WriteQueueSize),
		cb:      concurrency.NewCircuitBreaker(50, 20, 15*time.Second),
	}
}

func (s *Storage) Connect() error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		s.config.Host, s.config.Port, s.config.User, s.config.Password, s.config.DBName, s.config.SSLMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(s.config.MaxOpenConns)
	db.SetMaxIdleConns(s.config.MaxIdleConns)
	db.SetConnMaxLifetime(s.config.ConnMaxLifetime)

	if err := db.PingContext(s.ctx); err != nil {
		db.Close()
		return fmt.Errorf("ping database: %w", err)
	}

	s.db = db
	return nil
}

func (s *Storage) Start() {
	for i := 0; i < s.config.MaxWriteWorkers; i++ {
		s.wg.Add(1)
		go s.writeWorker(i)
	}
	s.wg.Add(1)
	go s.dispatchWorker()
}

func (s *Storage) dispatchWorker() {
	defer s.wg.Done()
	defer close(s.writeCh)

	for {
		select {
		case <-s.ctx.Done():
			return
		case batch, ok := <-s.input:
			if !ok {
				return
			}
			if len(batch) == 0 {
				continue
			}

			if !s.cb.Allow() {
				atomic.AddUint64(&s.batchesDropped, 1)
				continue
			}

			select {
			case s.writeCh <- batch:
			default:
				atomic.AddUint64(&s.batchesDropped, 1)
				s.cb.Failure()
			}
		}
	}
}

func (s *Storage) writeWorker(id int) {
	defer s.wg.Done()

	for batch := range s.writeCh {
		s.writeBatchWithRetry(batch)
	}
}

func (s *Storage) writeBatchWithRetry(batch []*model.VesselData) {
	var lastErr error
	start := time.Now()

	for attempt := 0; attempt < s.config.RetryCount; attempt++ {
		if err := s.ctx.Err(); err != nil {
			return
		}

		writeCtx, cancel := context.WithTimeout(s.ctx, s.config.WriteTimeout)
		err := s.writeBatchCopyIn(writeCtx, batch)
		cancel()

		if err != nil {
			lastErr = err
			time.Sleep(s.config.RetryWait)
			continue
		}

		duration := time.Since(start)
		atomic.AddUint64(&s.batchesWritten, 1)
		atomic.AddUint64(&s.recordsWritten, uint64(len(batch)))
		atomic.StoreInt64(&s.lastWriteTime, time.Now().UTC().UnixNano())
		atomic.StoreInt64(&s.lastWriteDuration, int64(duration))
		s.cb.Success()
		return
	}

	atomic.AddUint64(&s.batchesFailed, 1)
	s.cb.Failure()
	_ = lastErr
}

func (s *Storage) writeBatchCopyIn(ctx context.Context, batch []*model.VesselData) error {
	txn, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(
		"vessel_positions",
		"time", "mmsi", "latitude", "longitude", "cog", "sog", "message_type",
	))
	if err != nil {
		return fmt.Errorf("prepare copy: %w", err)
	}
	defer stmt.Close()

	for _, data := range batch {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := stmt.ExecContext(ctx,
			data.Timestamp,
			data.MMSI,
			data.Latitude,
			data.Longitude,
			data.COG,
			data.SOG,
			data.MessageType,
		)
		if err != nil {
			return fmt.Errorf("copy exec: %w", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("copy finish: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (s *Storage) GetStats() StorageStats {
	stateStr := "CLOSED"
	switch s.cb.State() {
	case concurrency.StateOpen:
		stateStr = "OPEN"
	case concurrency.StateHalfOpen:
		stateStr = "HALF_OPEN"
	}

	return StorageStats{
		BatchesWritten:    atomic.LoadUint64(&s.batchesWritten),
		RecordsWritten:    atomic.LoadUint64(&s.recordsWritten),
		BatchesFailed:     atomic.LoadUint64(&s.batchesFailed),
		BatchesDropped:    atomic.LoadUint64(&s.batchesDropped),
		LastWriteTime:     time.Unix(0, atomic.LoadInt64(&s.lastWriteTime)),
		LastWriteDuration: time.Duration(atomic.LoadInt64(&s.lastWriteDuration)),
		WriteQueueDepth:   len(s.writeCh),
		ActiveWorkers:     s.config.MaxWriteWorkers,
		CircuitState:      stateStr,
	}
}

func (s *Storage) Stop() error {
	s.cancel()
	s.wg.Wait()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Storage) InitSchema() error {
	schemaSQL := `
	CREATE TABLE IF NOT EXISTS vessel_positions (
		time TIMESTAMPTZ NOT NULL,
		mmsi BIGINT NOT NULL,
		latitude DOUBLE PRECISION NOT NULL,
		longitude DOUBLE PRECISION NOT NULL,
		cog DOUBLE PRECISION,
		sog DOUBLE PRECISION,
		message_type SMALLINT
	);

	SELECT create_hypertable('vessel_positions', 'time', if_not_exists => TRUE);
	CREATE INDEX IF NOT EXISTS idx_vessel_positions_mmsi_time ON vessel_positions (mmsi, time DESC);
	`
	_, err := s.db.ExecContext(s.ctx, schemaSQL)
	return err
}
