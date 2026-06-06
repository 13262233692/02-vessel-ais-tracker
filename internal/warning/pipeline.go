package warning

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/vessel-ais-tracker/internal/geofence"
	"github.com/vessel-ais-tracker/internal/geospatial"
	"github.com/vessel-ais-tracker/internal/model"
)

type Config struct {
	WorkerCount    int
	InputQueueSize int
	AlertQueueSize int
	MaxVessels     int
}

type WarningPipeline struct {
	config     Config
	input      <-chan *model.VesselData
	alertCh    chan *Alert
	fenceMgr   *geofence.Manager
	projectors []*TrajectoryProjector
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	processed  uint64
	dropped    uint64
}

func New(config Config, input <-chan *model.VesselData, fenceMgr *geofence.Manager) *WarningPipeline {
	ctx, cancel := context.WithCancel(context.Background())

	if config.WorkerCount <= 0 {
		config.WorkerCount = 4
	}
	if config.InputQueueSize <= 0 {
		config.InputQueueSize = 50000
	}
	if config.AlertQueueSize <= 0 {
		config.AlertQueueSize = 1000
	}

	alertCh := make(chan *Alert, config.AlertQueueSize)

	projectors := make([]*TrajectoryProjector, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		projectors[i] = NewTrajectoryProjector(fenceMgr, alertCh)
	}

	return &WarningPipeline{
		config:     config,
		input:      input,
		alertCh:    alertCh,
		fenceMgr:   fenceMgr,
		projectors: projectors,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (wp *WarningPipeline) AlertChannel() <-chan *Alert {
	return wp.alertCh
}

func (wp *WarningPipeline) Start() {
	for i := 0; i < wp.config.WorkerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

func (wp *WarningPipeline) worker(id int) {
	defer wp.wg.Done()

	projector := wp.projectors[id]

	for {
		select {
		case <-wp.ctx.Done():
			return
		case data, ok := <-wp.input:
			if !ok {
				return
			}
			projector.ProcessVessel(data)
			atomic.AddUint64(&wp.processed, 1)
		}
	}
}

func TapInto(parsedChan <-chan *model.ParsedMessage, queueSize int) chan *model.VesselData {
	tapCh := make(chan *model.VesselData, queueSize)

	go func() {
		for msg := range parsedChan {
			if msg.Error != nil || msg.Data == nil {
				continue
			}
			select {
			case tapCh <- msg.Data:
			default:
			}
		}
		close(tapCh)
	}()

	return tapCh
}

type WarningStats struct {
	Processed     uint64
	Dropped       uint64
	AlertsSent    uint64
	ChecksDone    uint64
	ActiveVessels int
	FenceCount    int
}

func (wp *WarningPipeline) GetStats() WarningStats {
	var totalAlerts uint64
	var totalChecks uint64
	var maxVessels int

	for _, p := range wp.projectors {
		alerts, checks, vessels := p.GetStats()
		totalAlerts += alerts
		totalChecks += checks
		if vessels > maxVessels {
			maxVessels = vessels
		}
	}

	return WarningStats{
		Processed:     atomic.LoadUint64(&wp.processed),
		Dropped:       atomic.LoadUint64(&wp.dropped),
		AlertsSent:    totalAlerts,
		ChecksDone:    totalChecks,
		ActiveVessels: maxVessels,
		FenceCount:    wp.fenceMgr.FenceCount(),
	}
}

func (s WarningStats) String() string {
	return fmt.Sprintf("processed=%d dropped=%d alerts=%d checks=%d vessels=%d fences=%d",
		s.Processed, s.Dropped, s.AlertsSent, s.ChecksDone, s.ActiveVessels, s.FenceCount)
}

func (wp *WarningPipeline) Stop() {
	wp.cancel()
	wp.wg.Wait()
	close(wp.alertCh)
}

func LoadSampleFences(fenceMgr *geofence.Manager) {
	fence1 := &geofence.Geofence{
		ID:       "no-entry-001",
		Name:     "Shanghai Port Anchorage",
		Type:     geofence.TypeNoEntryZone,
		Priority: 1,
		Polygon: geospatial.NewPolygon([]geospatial.Point{
			{Lng: 121.8, Lat: 31.1},
			{Lng: 122.0, Lat: 31.1},
			{Lng: 122.0, Lat: 31.3},
			{Lng: 121.8, Lat: 31.3},
		}),
	}

	fence2 := &geofence.Geofence{
		ID:       "reef-001",
		Name:     "Dangerous Reef East",
		Type:     geofence.TypeReefZone,
		Priority: 2,
		Polygon: geospatial.NewPolygon([]geospatial.Point{
			{Lng: 122.5, Lat: 30.8},
			{Lng: 122.7, Lat: 30.8},
			{Lng: 122.7, Lat: 31.0},
			{Lng: 122.5, Lat: 31.0},
		}),
	}

	fence3 := &geofence.Geofence{
		ID:       "restricted-001",
		Name:     "Military Restricted Zone",
		Type:     geofence.TypeRestrictedZone,
		Priority: 3,
		Polygon: geospatial.NewPolygon([]geospatial.Point{
			{Lng: 121.5, Lat: 31.5},
			{Lng: 121.7, Lat: 31.5},
			{Lng: 121.7, Lat: 31.7},
			{Lng: 121.5, Lat: 31.7},
		}),
	}

	fenceMgr.AddFence(fence1)
	fenceMgr.AddFence(fence2)
	fenceMgr.AddFence(fence3)
}
