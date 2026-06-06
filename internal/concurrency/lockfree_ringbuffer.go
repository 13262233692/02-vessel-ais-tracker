package concurrency

import (
	"runtime"
	"sync/atomic"
)

const (
	cacheLinePad = 64
)

type LockFreeRingBuffer[T any] struct {
	_          [cacheLinePad]byte
	capacity   uint64
	mask       uint64
	_          [cacheLinePad - 8]byte
	readIndex  uint64
	_          [cacheLinePad - 8]byte
	writeIndex uint64
	_          [cacheLinePad - 8]byte
	buffer     []node[T]
	_          [cacheLinePad]byte
}

type node[T any] struct {
	seq  uint64
	data T
}

func NewLockFreeRingBuffer[T any](capacity uint64) *LockFreeRingBuffer[T] {
	capacity = nextPowerOfTwo(capacity)
	rb := &LockFreeRingBuffer[T]{
		capacity: capacity,
		mask:     capacity - 1,
		buffer:   make([]node[T], capacity),
	}
	for i := range rb.buffer {
		rb.buffer[i].seq = uint64(i)
	}
	return rb
}

func (rb *LockFreeRingBuffer[T]) Push(item T) bool {
	for {
		writeIdx := atomic.LoadUint64(&rb.writeIndex)
		nodeIdx := writeIdx & rb.mask
		seq := atomic.LoadUint64(&rb.buffer[nodeIdx].seq)

		if seq == writeIdx {
			if atomic.CompareAndSwapUint64(&rb.writeIndex, writeIdx, writeIdx+1) {
				rb.buffer[nodeIdx].data = item
				atomic.StoreUint64(&rb.buffer[nodeIdx].seq, writeIdx+1)
				return true
			}
		} else if seq < writeIdx {
			return false
		} else {
			runtime.Gosched()
		}
	}
}

func (rb *LockFreeRingBuffer[T]) Pop() (T, bool) {
	for {
		readIdx := atomic.LoadUint64(&rb.readIndex)
		nodeIdx := readIdx & rb.mask
		seq := atomic.LoadUint64(&rb.buffer[nodeIdx].seq)

		if seq == readIdx+1 {
			if atomic.CompareAndSwapUint64(&rb.readIndex, readIdx, readIdx+1) {
				data := rb.buffer[nodeIdx].data
				var zero T
				rb.buffer[nodeIdx].data = zero
				atomic.StoreUint64(&rb.buffer[nodeIdx].seq, readIdx+rb.capacity)
				return data, true
			}
		} else if seq < readIdx+1 {
			var zero T
			return zero, false
		} else {
			runtime.Gosched()
		}
	}
}

func (rb *LockFreeRingBuffer[T]) PopBatch(dst []T, maxSize int) int {
	count := 0
	for count < maxSize {
		item, ok := rb.Pop()
		if !ok {
			break
		}
		dst[count] = item
		count++
	}
	return count
}

func (rb *LockFreeRingBuffer[T]) Len() int {
	writeIdx := atomic.LoadUint64(&rb.writeIndex)
	readIdx := atomic.LoadUint64(&rb.readIndex)
	return int(writeIdx - readIdx)
}

func (rb *LockFreeRingBuffer[T]) Cap() int {
	return int(rb.capacity)
}

func (rb *LockFreeRingBuffer[T]) IsEmpty() bool {
	return rb.Len() == 0
}

func (rb *LockFreeRingBuffer[T]) IsFull() bool {
	return rb.Len() >= int(rb.capacity)
}

func nextPowerOfTwo(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
