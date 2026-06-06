package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
)

type WorkerPool[T any, R any] struct {
	workerCount int
	taskCh      chan T
	resultCh    chan R
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	active      int32
	processFn   func(context.Context, T) R
}

func NewWorkerPool[T any, R any](
	ctx context.Context,
	workerCount int,
	queueSize int,
	processFn func(context.Context, T) R,
) *WorkerPool[T, R] {
	ctx, cancel := context.WithCancel(ctx)
	return &WorkerPool[T, R]{
		workerCount: workerCount,
		taskCh:      make(chan T, queueSize),
		resultCh:    make(chan R, queueSize),
		ctx:         ctx,
		cancel:      cancel,
		processFn:   processFn,
	}
}

func (p *WorkerPool[T, R]) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool[T, R]) worker() {
	defer p.wg.Done()
	atomic.AddInt32(&p.active, 1)
	defer atomic.AddInt32(&p.active, -1)

	for {
		select {
		case <-p.ctx.Done():
			return
		case task, ok := <-p.taskCh:
			if !ok {
				return
			}
			result := p.processFn(p.ctx, task)
			select {
			case p.resultCh <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *WorkerPool[T, R]) Submit(task T) bool {
	select {
	case p.taskCh <- task:
		return true
	default:
		return false
	}
}

func (p *WorkerPool[T, R]) SubmitWithContext(ctx context.Context, task T) error {
	select {
	case p.taskCh <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *WorkerPool[T, R]) Results() <-chan R {
	return p.resultCh
}

func (p *WorkerPool[T, R]) ActiveWorkers() int {
	return int(atomic.LoadInt32(&p.active))
}

func (p *WorkerPool[T, R]) QueueDepth() int {
	return len(p.taskCh)
}

func (p *WorkerPool[T, R]) Stop() {
	p.cancel()
	close(p.taskCh)
	p.wg.Wait()
	close(p.resultCh)
}

type BoundedExecutor struct {
	semaphore *Semaphore
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewBoundedExecutor(ctx context.Context, maxConcurrency int) *BoundedExecutor {
	ctx, cancel := context.WithCancel(ctx)
	return &BoundedExecutor{
		semaphore: NewSemaphore(maxConcurrency),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (e *BoundedExecutor) Execute(fn func()) bool {
	if !e.semaphore.TryAcquire() {
		return false
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.semaphore.Release()
		select {
		case <-e.ctx.Done():
			return
		default:
			fn()
		}
	}()
	return true
}

func (e *BoundedExecutor) ExecuteWait(fn func()) error {
	if err := e.semaphore.Acquire(e.ctx); err != nil {
		return err
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.semaphore.Release()
		fn()
	}()
	return nil
}

func (e *BoundedExecutor) Wait() {
	e.wg.Wait()
}

func (e *BoundedExecutor) Stop() {
	e.cancel()
	e.wg.Wait()
}

func (e *BoundedExecutor) AvailablePermits() int {
	return e.semaphore.Available()
}

func (e *BoundedExecutor) MaxConcurrency() int {
	return cap(e.semaphore.permits)
}
