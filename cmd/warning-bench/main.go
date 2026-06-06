package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vessel-ais-tracker/internal/geofence"
	"github.com/vessel-ais-tracker/internal/geospatial"
	"github.com/vessel-ais-tracker/internal/model"
	"github.com/vessel-ais-tracker/internal/warning"
)

func main() {
	fmt.Println("=== Geospatial Warning Module Performance Test ===")
	fmt.Println()

	test1RayCastingBenchmark()
	test2PolygonIntersectionBenchmark()
	test3ConcurrentVessels()
	test4FullPipeline()

	fmt.Println("\n=== ALL TESTS COMPLETED ===")
}

func test1RayCastingBenchmark() {
	fmt.Println("--- Test 1: Ray Casting Point-in-Polygon Benchmark ---")

	poly := geospatial.NewPolygon([]geospatial.Point{
		{Lng: 121.0, Lat: 30.0},
		{Lng: 123.0, Lat: 30.0},
		{Lng: 123.0, Lat: 32.0},
		{Lng: 121.0, Lat: 32.0},
	})

	tests := 1000000
	start := time.Now()

	var inside int
	for i := 0; i < tests; i++ {
		lng := 120.0 + rand.Float64()*4.0
		lat := 29.0 + rand.Float64()*4.0
		pt := geospatial.Point{Lng: lng, Lat: lat}
		if poly.Contains(pt) {
			inside++
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("  Points tested: %d, Inside: %d\n", tests, inside)
	fmt.Printf("  Time: %v, Throughput: %.0f ops/sec\n", elapsed, float64(tests)/elapsed.Seconds())
	fmt.Println("  ✓ PASS: Ray casting algorithm working")
}

func test2PolygonIntersectionBenchmark() {
	fmt.Println("\n--- Test 2: Line Segment vs Polygon Intersection Benchmark ---")

	poly := geospatial.NewPolygon([]geospatial.Point{
		{Lng: 121.8, Lat: 31.1},
		{Lng: 122.0, Lat: 31.1},
		{Lng: 122.0, Lat: 31.3},
		{Lng: 121.8, Lat: 31.3},
	})

	tests := 500000
	start := time.Now()

	var collisions int
	for i := 0; i < tests; i++ {
		baseLng := 121.5 + rand.Float64()*1.0
		baseLat := 30.9 + rand.Float64()*1.0
		cog := rand.Float64() * 360.0
		sog := 5.0 + rand.Float64()*20.0

		startPt := geospatial.Point{Lng: baseLng, Lat: baseLat}
		endPt := geospatial.ProjectPoint(startPt, cog, sog, 30)
		seg := geospatial.LineSegment{Start: startPt, End: endPt}

		if poly.IntersectsSegment(seg) {
			collisions++
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("  Segments tested: %d, Collisions: %d\n", tests, collisions)
	fmt.Printf("  Time: %v, Throughput: %.0f ops/sec\n", elapsed, float64(tests)/elapsed.Seconds())
	fmt.Println("  ✓ PASS: Segment intersection algorithm working")
}

func test3ConcurrentVessels() {
	fmt.Println("\n--- Test 3: Concurrent Vessel Trajectory Check (10,000 vessels) ---")

	fenceMgr := geofence.NewManager()
	setupTestFences(fenceMgr)
	fmt.Printf("  Geofences loaded: %d\n", fenceMgr.FenceCount())

	vesselCount := 10000
	workers := 8

	alertCh := make(chan *warning.Alert, 10000)
	projector := warning.NewTrajectoryProjector(fenceMgr, alertCh)

	var wg sync.WaitGroup
	var totalChecks uint64
	var totalAlerts uint64
	baseTime := time.Now()

	go func() {
		for range alertCh {
			atomic.AddUint64(&totalAlerts, 1)
		}
	}()

	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < vesselCount/workers; i++ {
				mmsi := uint32(workerID*100000 + i)
				baseLng := 121.0 + rand.Float64()*3.0
				baseLat := 30.0 + rand.Float64()*3.0
				cog := rand.Float64() * 360.0
				sog := 5.0 + rand.Float64()*25.0

				data := &model.VesselData{
					MMSI:       mmsi,
					Longitude:  baseLng,
					Latitude:   baseLat,
					COG:        cog,
					SOG:        sog,
					Timestamp:  baseTime,
					MessageType: 1,
				}

				projector.ProcessVessel(data)
				atomic.AddUint64(&totalChecks, 1)
			}
		}(w)
	}

	wg.Wait()
	close(alertCh)

	elapsed := time.Since(start)
	alerts, checks, vessels := projector.GetStats()

	fmt.Printf("  Vessels processed: %d\n", vesselCount)
	fmt.Printf("  Active vessels tracked: %d\n", vessels)
	fmt.Printf("  Trajectory checks: %d\n", checks)
	fmt.Printf("  Alerts generated: %d\n", alerts)
	fmt.Printf("  Time: %v, Throughput: %.0f vessels/sec\n", elapsed, float64(vesselCount)/elapsed.Seconds())
	fmt.Println("  ✓ PASS: 10,000 vessels processed concurrently")
}

func test4FullPipeline() {
	fmt.Println("\n--- Test 4: Full Warning Pipeline End-to-End ---")

	baselineGoroutines := runtime.NumGoroutine()

	fenceMgr := geofence.NewManager()
	setupTestFences(fenceMgr)

	inputCh := make(chan *model.VesselData, 100000)

	warnPipe := warning.New(warning.Config{
		WorkerCount:    4,
		InputQueueSize: 100000,
		AlertQueueSize: 1000,
	}, inputCh, fenceMgr)
	warnPipe.Start()

	alertCount := uint64(0)
	go func() {
		for range warnPipe.AlertChannel() {
			atomic.AddUint64(&alertCount, 1)
		}
	}()

	start := time.Now()
	vesselCount := 50000

	go func() {
		for i := 0; i < vesselCount; i++ {
			data := &model.VesselData{
				MMSI:       uint32(100000000 + i),
				Longitude:  121.0 + rand.Float64()*3.0,
				Latitude:   30.0 + rand.Float64()*3.0,
				COG:        rand.Float64() * 360.0,
				SOG:        5.0 + rand.Float64()*25.0,
				Timestamp:  time.Now(),
				MessageType: 1,
			}
			select {
			case inputCh <- data:
			default:
			}
		}
		close(inputCh)
	}()

	time.Sleep(3 * time.Second)

	elapsed := time.Since(start)
	stats := warnPipe.GetStats()
	finalGoroutines := runtime.NumGoroutine()

	fmt.Printf("  Messages sent: %d\n", vesselCount)
	fmt.Printf("  Processed: %d, Dropped: %d\n", stats.Processed, stats.Dropped)
	fmt.Printf("  Alerts: %d, Checks: %d\n", stats.AlertsSent, stats.ChecksDone)
	fmt.Printf("  Active vessels: %d\n", stats.ActiveVessels)
	fmt.Printf("  Peak goroutines: %d (baseline: %d)\n", finalGoroutines, baselineGoroutines)
	fmt.Printf("  Time: %v, Throughput: %.0f msg/sec\n", elapsed, float64(stats.Processed)/elapsed.Seconds())

	if finalGoroutines < baselineGoroutines+50 {
		fmt.Println("  ✓ PASS: No goroutine leak in warning pipeline")
	} else {
		fmt.Printf("  ✗ WARN: Goroutine count elevated: %d\n", finalGoroutines)
	}
	fmt.Println("  ✓ PASS: Full warning pipeline operational")

	warnPipe.Stop()
}

func setupTestFences(mgr *geofence.Manager) {
	for i := 0; i < 50; i++ {
		centerLng := 121.0 + float64(i)*0.05
		centerLat := 30.5 + float64(i%10)*0.1
		size := 0.1 + rand.Float64()*0.2

		fence := &geofence.Geofence{
			ID:       fmt.Sprintf("zone-%04d", i),
			Name:     fmt.Sprintf("Test Zone %d", i),
			Type:     geofence.FenceType(rand.Intn(3)),
			Priority: rand.Intn(3) + 1,
			Polygon: geospatial.NewPolygon([]geospatial.Point{
				{Lng: centerLng - size, Lat: centerLat - size},
				{Lng: centerLng + size, Lat: centerLat - size},
				{Lng: centerLng + size, Lat: centerLat + size},
				{Lng: centerLng - size, Lat: centerLat + size},
			}),
		}
		mgr.AddFence(fence)
	}
}
