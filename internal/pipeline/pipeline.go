package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vessel-ais-tracker/internal/concurrency"
	"github.com/vessel-ais-tracker/internal/model"
)

type Config struct {
	RingBufferSize int
	BatchSize      int
	FlushInterval  time.Duration
	EnqueueWorkers int
}

type Pipeline struct {
	config     Config
	ring       *concurrency.LockFreeRingBuffer[*model.VesselData]
	input      <-chan *model.ParsedMessage
	output     chan<- []*model.VesselData
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	enqueued   uint64
	dequeued   uint64
	dropped    uint64
	batches    uint64
	cb         *concurrency.CircuitBreaker
}

type PipelineStats struct {
	Enqueued         uint64
	Dequeued         uint64
	Dropped          uint64
	BatchesFlushed   uint64
	CurrentQueueDepth int
	CircuitState     string
}

func New(config Config, input <-chan *model.ParsedMessage, output chan<- []*model.VesselData) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	if config.EnqueueWorkers <= 0 {
		config.EnqueueWorkers = 2
	}

	return &Pipeline{
		config: config,
		ring:   concurrency.NewLockFreeRingBuffer[*model.VesselData](uint64(config.RingBufferSize)),
		input:  input,
		output: output,
		ctx:    ctx,
		cancel: cancel,
		cb:     concurrency.NewCircuitBreaker(10, 5, 5*time.Second),
	}
}

func (p *Pipeline) Start() {
	for i := 0; i < p.config.EnqueueWorkers; i++ {
		p.wg.Add(1)
		go p.enqueueWorker()
	}
	p.wg.Add(1)
	go p.dequeueWorker()
}

func (p *Pipeline) enqueueWorker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.input:
			if !ok {
				return
			}
			if msg.Error != nil || msg.Data == nil {
				atomic.AddUint64(&p.dropped, 1)
				continue
			}
			if !p.cb.Allow() {
				atomic.AddUint64(&p.dropped, 1)
				continue
			}
			if p.ring.Push(msg.Data) {
				atomic.AddUint64(&p.enqueued, 1)
				p.cb.Success()
			} else {
				atomic.AddUint64(&p.dropped, 1)
				p.cb.Failure()
			}
		}
	}
}

func (p *Pipeline) dequeueWorker() {
	defer p.wg.Done()

	batch := make([]*model.VesselData, p.config.BatchSize)
	ticker := time.NewTicker(p.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.flushRemaining(batch)
			return
		case <-ticker.C:
			p.flushIfNeeded(batch)
		default:
			if p.ring.Len() >= p.config.BatchSize {
				p.flushBatch(batch)
			} else {
				select {
				case <-time.After(500 * time.Microsecond):
				case <-p.ctx.Done():
				}
			}
		}
	}
}

func (p *Pipeline) flushBatch(batch []*model.VesselData) {
	count := p.ring.PopBatch(batch, p.config.BatchSize)
	if count == 0 {
		return
	}

	dataBatch := make([]*model.VesselData, count)
	copy(dataBatch, batch[:count])

	select {
	case p.output <- dataBatch:
		atomic.AddUint64(&p.dequeued, uint64(count))
		atomic.AddUint64(&p.batches, 1)
	case <-p.ctx.Done():
	}
}

func (p *Pipeline) flushIfNeeded(batch []*model.VesselData) {
	if p.ring.Len() > 0 {
		p.flushBatch(batch)
	}
}

func (p *Pipeline) flushRemaining(batch []*model.VesselData) {
	for !p.ring.IsEmpty() {
		count := p.ring.PopBatch(batch, p.config.BatchSize)
		if count == 0 {
			break
		}
		dataBatch := make([]*model.VesselData, count)
		copy(dataBatch, batch[:count])
		select {
		case p.output <- dataBatch:
			atomic.AddUint64(&p.dequeued, uint64(count))
			atomic.AddUint64(&p.batches, 1)
		default:
		}
	}
}

func (p *Pipeline) GetStats() PipelineStats {
	stateStr := "CLOSED"
	switch p.cb.State() {
	case concurrency.StateOpen:
		stateStr = "OPEN"
	case concurrency.StateHalfOpen:
		stateStr = "HALF_OPEN"
	}

	return PipelineStats{
		Enqueued:         atomic.LoadUint64(&p.enqueued),
		Dequeued:         atomic.LoadUint64(&p.dequeued),
		Dropped:          atomic.LoadUint64(&p.dropped),
		BatchesFlushed:   atomic.LoadUint64(&p.batches),
		CurrentQueueDepth: p.ring.Len(),
		CircuitState:     stateStr,
	}
}

func (p *Pipeline) Stop() error {
	p.cancel()
	p.wg.Wait()
	return nil
}

func (s PipelineStats) String() string {
	return fmt.Sprintf("Enqueued=%d Dequeued=%d Dropped=%d Batches=%d QueueDepth=%d Circuit=%s",
		s.Enqueued, s.Dequeued, s.Dropped, s.BatchesFlushed, s.CurrentQueueDepth, s.CircuitState)
}
