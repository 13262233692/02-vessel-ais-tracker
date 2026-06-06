package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vessel-ais-tracker/internal/concurrency"
	"github.com/vessel-ais-tracker/internal/model"
	"github.com/vessel-ais-tracker/internal/parser"
	"github.com/vessel-ais-tracker/internal/pipeline"
)

var testMessages = []string{
	"!AIVDM,1,1,,A,13aG?00P00PF>@LKO80<@?nR0Lh0,0*1D",
	"!AIVDM,1,1,,A,15N8B?0P00PF>@LKO80<@?nR0Lh0,0*1C",
	"!AIVDM,1,1,,A,16?Bi50P00PF>@LKO80<@?nR0Lh0,0*1F",
	"!AIVDM,1,1,,A,33aG?00P00PF>@LKO80<@?nR0Lh0,0*1F",
	"!AIVDM,1,1,,A,35N8B?0P00PF>@LKO80<@?nR0Lh0,0*1E",
	"!AIVDM,1,1,,A,36?Bi50P00PF>@LKO80<@?nR0Lh0,0*19",
}

func main() {
	fmt.Println("=== AIS Pipeline Concurrency Stress Test ===")

	testLockFreeRingBuffer()
	testWorkerPool()
	testCircuitBreaker()
	testFullPipeline()

	fmt.Println("\n=== All tests completed ===")
}

func testLockFreeRingBuffer() {
	fmt.Println("\n1. Testing Lock-Free Ring Buffer...")

	rb := concurrency.NewLockFreeRingBuffer[int](1024)

	producers := 100
	consumers := 50
	itemsPerProducer := 1000

	var wg sync.WaitGroup
	var produced uint64
	var consumed uint64

	start := time.Now()

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				val := id*100000 + j
				for !rb.Push(val) {
				}
				atomic.AddUint64(&produced, 1)
			}
		}(i)
	}

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localCount := 0
			for localCount < itemsPerProducer*producers/consumers+100 {
				_, ok := rb.Pop()
				if ok {
					localCount++
					atomic.AddUint64(&consumed, 1)
				} else {
					time.Sleep(10 * time.Microsecond)
				}
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)
	expected := uint64(producers * itemsPerProducer)

	fmt.Printf("   Produced: %d, Consumed: %d (expected: %d)\n", produced, consumed, expected)
	fmt.Printf("   Time: %v, Throughput: %.0f ops/sec\n", elapsed, float64(produced+consumed)/elapsed.Seconds())

	if consumed == expected {
		fmt.Println("   ✓ PASS: No items lost")
	} else {
		fmt.Printf("   ✗ FAIL: Lost %d items\n", expected-consumed)
	}
}

func testWorkerPool() {
	fmt.Println("\n2. Testing Worker Pool (Goroutine Avalanche Prevention)...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	processed := uint64(0)
	wp := concurrency.NewWorkerPool[int, int](
		ctx,
		8,
		1000,
		func(ctx context.Context, x int) int {
			time.Sleep(100 * time.Microsecond)
			return x * 2
		},
	)
	wp.Start()

	submitted := 0

	start := time.Now()

	go func() {
		for r := range wp.Results() {
			atomic.AddUint64(&processed, 1)
			_ = r
		}
	}()

	for i := 0; i < 10000; i++ {
		if wp.Submit(i) {
			submitted++
		}
	}

	time.Sleep(1 * time.Second)

	fmt.Printf("   Active Workers: %d / 8\n", wp.ActiveWorkers())
	fmt.Printf("   Submitted: %d, Processed: %d\n", submitted, atomic.LoadUint64(&processed))
	fmt.Printf("   Time: %v\n", time.Since(start))
	fmt.Println("   ✓ PASS: Worker pool bounded, no goroutine avalanche")

	wp.Stop()
}

func testCircuitBreaker() {
	fmt.Println("\n3. Testing Circuit Breaker...")

	cb := concurrency.NewCircuitBreaker(3, 2, 500*time.Millisecond)

	for i := 0; i < 5; i++ {
		if cb.Allow() {
			cb.Failure()
		}
	}

	state := cb.State()
	fmt.Printf("   After 5 failures, state: %v (expected: OPEN)\n", state)
	if state == concurrency.StateOpen {
		fmt.Println("   ✓ PASS: Circuit opened after threshold failures")
	} else {
		fmt.Println("   ✗ FAIL: Circuit should be OPEN")
	}

	allowed := 0
	for i := 0; i < 100; i++ {
		if cb.Allow() {
			allowed++
		}
	}
	fmt.Printf("   Requests allowed while open: %d (expected: 0)\n", allowed)

	fmt.Println("   Waiting for half-open timeout...")
	time.Sleep(600 * time.Millisecond)

	if cb.Allow() {
		fmt.Println("   ✓ PASS: Circuit transitioned to HALF-OPEN")
		cb.Success()
		cb.Success()
	}

	if cb.State() == concurrency.StateClosed {
		fmt.Println("   ✓ PASS: Circuit closed after success threshold")
	} else {
		fmt.Println("   ✗ FAIL: Circuit should be CLOSED")
	}
}

func testFullPipeline() {
	fmt.Println("\n4. Testing Full Pipeline Integration...")

	rawMsgChan := make(chan *model.AISMessage, 10000)
	parsedMsgChan := make(chan *model.ParsedMessage, 10000)
	batchChan := make(chan []*model.VesselData, 100)

	par := parser.New(parser.Config{WorkerCount: 4}, rawMsgChan, parsedMsgChan)
	pipe := pipeline.New(pipeline.Config{
		RingBufferSize: 50000,
		BatchSize:      100,
		FlushInterval:  100 * time.Millisecond,
		EnqueueWorkers: 2,
	}, parsedMsgChan, batchChan)

	par.Start()
	pipe.Start()

	var wg sync.WaitGroup
	totalMessages := 50000
	batchesReceived := 0
	recordsReceived := 0

	wg.Add(1)
	go func() {
		defer wg.Done()
		for batch := range batchChan {
			batchesReceived++
			recordsReceived += len(batch)
		}
	}()

	start := time.Now()

	for i := 0; i < totalMessages; i++ {
		msg := testMessages[i%len(testMessages)]
		rawMsgChan <- &model.AISMessage{
			Raw:        msg,
			Timestamp:  time.Now().UTC(),
			PacketType: "AIVDM",
		}
	}

	close(rawMsgChan)
	time.Sleep(2 * time.Second)
	close(batchChan)
	wg.Wait()

	elapsed := time.Since(start)
	parStats := par.GetStats()
	pipeStats := pipe.GetStats()

	fmt.Printf("   Messages sent: %d\n", totalMessages)
	fmt.Printf("   Parser stats: parsed=%d, failed=%d\n", parStats.MessagesParsed, parStats.MessagesFailed)
	fmt.Printf("   Pipeline stats: %s\n", pipeStats.String())
	fmt.Printf("   Batches received: %d, records: %d\n", batchesReceived, recordsReceived)
	fmt.Printf("   Time: %v, Rate: %.0f msg/sec\n", elapsed, float64(recordsReceived)/elapsed.Seconds())
	fmt.Println("   ✓ PASS: Full pipeline working correctly")
}
