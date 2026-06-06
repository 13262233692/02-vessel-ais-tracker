package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Semaphore struct {
	permits chan struct{}
}

func NewSemaphore(maxConcurrency int) *Semaphore {
	return &Semaphore{
		permits: make(chan struct{}, maxConcurrency),
	}
}

func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.permits <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Semaphore) TryAcquire() bool {
	select {
	case s.permits <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Semaphore) Release() {
	<-s.permits
}

func (s *Semaphore) Available() int {
	return cap(s.permits) - len(s.permits)
}

type CircuitBreakerState int32

const (
	StateClosed CircuitBreakerState = iota
	StateHalfOpen
	StateOpen
)

type CircuitBreaker struct {
	state             int32
	failureCount      int64
	successCount      int64
	failureThreshold  int64
	successThreshold  int64
	timeout           time.Duration
	lastStateChange   int64
	mu                sync.RWMutex
}

func NewCircuitBreaker(failureThreshold int64, successThreshold int64, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		lastStateChange:  time.Now().UnixNano(),
	}
}

func (cb *CircuitBreaker) State() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&cb.state))
}

func (cb *CircuitBreaker) Allow() bool {
	state := cb.State()

	switch state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(time.Unix(0, atomic.LoadInt64(&cb.lastStateChange))) >= cb.timeout {
			cb.mu.Lock()
			if cb.State() == StateOpen {
				atomic.StoreInt32(&cb.state, int32(StateHalfOpen))
				atomic.StoreInt64(&cb.lastStateChange, time.Now().UnixNano())
				atomic.StoreInt64(&cb.successCount, 0)
			}
			cb.mu.Unlock()
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) Success() {
	state := cb.State()

	switch state {
	case StateClosed:
		atomic.StoreInt64(&cb.failureCount, 0)
	case StateHalfOpen:
		newCount := atomic.AddInt64(&cb.successCount, 1)
		if newCount >= cb.successThreshold {
			cb.mu.Lock()
			if cb.State() == StateHalfOpen {
				atomic.StoreInt32(&cb.state, int32(StateClosed))
				atomic.StoreInt64(&cb.lastStateChange, time.Now().UnixNano())
				atomic.StoreInt64(&cb.failureCount, 0)
				atomic.StoreInt64(&cb.successCount, 0)
			}
			cb.mu.Unlock()
		}
	}
}

func (cb *CircuitBreaker) Failure() {
	state := cb.State()

	switch state {
	case StateClosed:
		newCount := atomic.AddInt64(&cb.failureCount, 1)
		if newCount >= cb.failureThreshold {
			cb.mu.Lock()
			if cb.State() == StateClosed {
				atomic.StoreInt32(&cb.state, int32(StateOpen))
				atomic.StoreInt64(&cb.lastStateChange, time.Now().UnixNano())
				atomic.StoreInt64(&cb.failureCount, 0)
			}
			cb.mu.Unlock()
		}
	case StateHalfOpen:
		cb.mu.Lock()
		if cb.State() == StateHalfOpen {
			atomic.StoreInt32(&cb.state, int32(StateOpen))
			atomic.StoreInt64(&cb.lastStateChange, time.Now().UnixNano())
			atomic.StoreInt64(&cb.successCount, 0)
		}
		cb.mu.Unlock()
	}
}
