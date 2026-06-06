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
	lastWriteTime     int64
	lastWriteDuration int64
	executor          *concurrency.BoundedExecutor
	cb                *concurrency.CircuitBreaker
}

type StorageStats struct {
	BatchesWritten      uint64
	RecordsWritten      uint64
	BatchesFailed       uint64
	LastWriteTime       time.Time
	LastWriteDuration   time.Duration
	ActiveWorkers       int
	PendingBatches      int
	CircuitState        string
}

func New(config Config, input <-chan []*model.VesselData) *Storage {
	ctx, cancel := context.WithCancel(context.Background())

	if config.MaxWriteWorkers <= 0 {
		config.MaxWriteWorkers = 4
	}
	if config.WriteQueueSize <= 0 {
		config.WriteQueueSize = 100
	}

	return &Storage{
		config:   config,
		input:    input,
		ctx:      ctx,
		cancel:   cancel,
		executor: concurrency.NewBoundedExecutor(ctx, config.MaxWriteWorkers),
		cb:       concurrency.NewCircuitBreaker(20, 10, 10*time.Second),
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
	s.wg.Add(1)
	go s.dispatchWorker()
}

func (s *Storage) dispatchWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			s.executor.Stop()
			return
		case batch, ok := <-s.input:
			if !ok {
				s.executor.Stop()
				return
			}
			if len(batch) == 0 {
				continue
			}

			if !s.cb.Allow() {
				atomic.AddUint64(&s.batchesFailed, 1)
				continue
			}

			batchCopy := batch
			submitted := s.executor.Execute(func() {
				s.writeBatchWithRetry(batchCopy)
			})

			if !submitted {
				atomic.AddUint64(&s.batchesFailed, 1)
				s.cb.Failure()
			}
		}
	}
}

func (s *Storage) writeBatchWithRetry(batch []*model.VesselData) {
	var lastErr error
	start := time.Now()

	for attempt := 0; attempt < s.config.RetryCount; attempt++ {
		if err := s.ctx.Err(); err != nil {
			return
		}

		if err := s.writeBatchCopyIn(batch); err != nil {
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

func (s *Storage) writeBatchCopyIn(batch []*model.VesselData) error {
	txn, err := s.db.BeginTx(s.ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.PrepareContext(s.ctx, pq.CopyIn(
		"vessel_positions",
		"time", "mmsi", "latitude", "longitude", "cog", "sog", "message_type",
	))
	if err != nil {
		return fmt.Errorf("prepare copy: %w", err)
	}
	defer stmt.Close()

	for _, data := range batch {
		_, err := stmt.ExecContext(s.ctx,
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

	if _, err := stmt.ExecContext(s.ctx); err != nil {
		return fmt.Errorf("copy finish: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (s *Storage) writeBatch(batch []*model.VesselData) error {
	tx, err := s.db.BeginTx(s.ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(s.ctx, `
		INSERT INTO vessel_positions (
			time, mmsi, latitude, longitude, cog, sog, message_type
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		return fmt.Errorf("prepare stmt: %w", err)
	}
	defer stmt.Close()

	for _, data := range batch {
		_, err := stmt.ExecContext(s.ctx,
			data.Timestamp,
			data.MMSI,
			data.Latitude,
			data.Longitude,
			data.COG,
			data.SOG,
			data.MessageType,
		)
		if err != nil {
			return fmt.Errorf("exec stmt: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
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
		LastWriteTime:     time.Unix(0, atomic.LoadInt64(&s.lastWriteTime)),
		LastWriteDuration: time.Duration(atomic.LoadInt64(&s.lastWriteDuration)),
		ActiveWorkers:     s.executor.MaxConcurrency() - s.executor.AvailablePermits(),
		PendingBatches:    len(s.input),
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
