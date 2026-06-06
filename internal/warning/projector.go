package warning

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vessel-ais-tracker/internal/geospatial"
	"github.com/vessel-ais-tracker/internal/geofence"
	"github.com/vessel-ais-tracker/internal/model"
)

const (
	ProjectionMinutes = 30
	CooldownSeconds   = 60
	LRUCacheSize      = 50000
)

type VesselState struct {
	MMSI          uint32
	LastUpdate    time.Time
	LastPosition  geospatial.Point
	COG           float64
	SOG           float64
	LastAlertTime map[string]time.Time
}

type Alert struct {
	ID         string
	VesselMMSI uint32
	FenceID    string
	FenceName  string
	FenceType  geofence.FenceType
	Priority   int
	Timestamp  time.Time
	Latitude   float64
	Longitude  float64
	COG        float64
	SOG        float64
	ETA        time.Time
}

type TrajectoryProjector struct {
	fenceManager *geofence.Manager
	states       *lruCache
	alertCh      chan<- *Alert
	alertsSent   uint64
	checksDone   uint64
}

func NewTrajectoryProjector(fenceManager *geofence.Manager, alertCh chan<- *Alert) *TrajectoryProjector {
	return &TrajectoryProjector{
		fenceManager: fenceManager,
		states:       newLRUCache(LRUCacheSize),
		alertCh:      alertCh,
	}
}

func (tp *TrajectoryProjector) ProcessVessel(data *model.VesselData) {
	if data == nil || data.MMSI == 0 {
		return
	}

	currentPos := geospatial.Point{Lng: data.Longitude, Lat: data.Latitude}

	state := tp.states.Get(data.MMSI)
	if state == nil {
		state = &VesselState{
			MMSI:          data.MMSI,
			LastAlertTime: make(map[string]time.Time),
		}
		tp.states.Put(data.MMSI, state)
	}

	state.LastUpdate = time.Now().UTC()
	state.LastPosition = currentPos
	state.COG = data.COG
	state.SOG = data.SOG

	atomic.AddUint64(&tp.checksDone, 1)

	if data.SOG < 0.1 {
		tp.checkCurrentPosition(state, currentPos)
		return
	}

	futurePos := geospatial.ProjectPoint(currentPos, data.COG, data.SOG, ProjectionMinutes)
	trajectory := geospatial.LineSegment{
		Start: currentPos,
		End:   futurePos,
	}

	collisions := tp.fenceManager.CheckCollision(trajectory)
	for _, fence := range collisions {
		tp.maybeSendAlert(state, fence, currentPos, data, futurePos)
	}
}

func (tp *TrajectoryProjector) checkCurrentPosition(state *VesselState, pos geospatial.Point) {
	fences := tp.fenceManager.CheckPointInFence(pos)
	for _, fence := range fences {
		data := &model.VesselData{
			Latitude:  pos.Lat,
			Longitude: pos.Lng,
			COG:       state.COG,
			SOG:       state.SOG,
		}
		tp.maybeSendAlert(state, fence, pos, data, pos)
	}
}

func (tp *TrajectoryProjector) maybeSendAlert(
	state *VesselState,
	fence *geofence.Geofence,
	currentPos geospatial.Point,
	data *model.VesselData,
	futurePos geospatial.Point,
) {
	lastAlert, exists := state.LastAlertTime[fence.ID]
	if exists && time.Since(lastAlert) < CooldownSeconds*time.Second {
		return
	}

	state.LastAlertTime[fence.ID] = time.Now().UTC()

	distance := geospatial.HaversineDistance(currentPos, futurePos)
	etaMinutes := 0.0
	if data.SOG > 0.1 {
		etaMinutes = (distance / (data.SOG * 1.852)) * 60
	}

	alert := &Alert{
		ID:         generateAlertID(state.MMSI, fence.ID),
		VesselMMSI: state.MMSI,
		FenceID:    fence.ID,
		FenceName:  fence.Name,
		FenceType:  fence.Type,
		Priority:   fence.Priority,
		Timestamp:  time.Now().UTC(),
		Latitude:   data.Latitude,
		Longitude:  data.Longitude,
		COG:        data.COG,
		SOG:        data.SOG,
		ETA:        time.Now().UTC().Add(time.Duration(etaMinutes * float64(time.Minute))),
	}

	select {
	case tp.alertCh <- alert:
		atomic.AddUint64(&tp.alertsSent, 1)
	default:
	}
}

func (tp *TrajectoryProjector) GetStats() (alertsSent uint64, checksDone uint64, activeVessels int) {
	return atomic.LoadUint64(&tp.alertsSent),
		atomic.LoadUint64(&tp.checksDone),
		tp.states.Len()
}

func generateAlertID(mmsi uint32, fenceID string) string {
	return string(rune(mmsi)) + ":" + fenceID + ":" + time.Now().UTC().Format("20060102150405")
}

type lruEntry struct {
	key   uint32
	value *VesselState
	prev  *lruEntry
	next  *lruEntry
}

type lruCache struct {
	capacity int
	items    map[uint32]*lruEntry
	head     *lruEntry
	tail     *lruEntry
	mu       sync.RWMutex
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		items:    make(map[uint32]*lruEntry, capacity),
	}
}

func (c *lruCache) Get(key uint32) *VesselState {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		return nil
	}

	c.mu.Lock()
	c.moveToFront(entry)
	c.mu.Unlock()

	return entry.value
}

func (c *lruCache) Put(key uint32, value *VesselState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		entry.value = value
		c.moveToFront(entry)
		return
	}

	if len(c.items) >= c.capacity {
		c.evict()
	}

	entry := &lruEntry{key: key, value: value}
	c.items[key] = entry
	c.addToFront(entry)
}

func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *lruCache) addToFront(entry *lruEntry) {
	entry.next = c.head
	entry.prev = nil
	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry
	if c.tail == nil {
		c.tail = entry
	}
}

func (c *lruCache) moveToFront(entry *lruEntry) {
	if entry == c.head {
		return
	}

	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	if entry == c.tail {
		c.tail = entry.prev
	}

	entry.prev = nil
	entry.next = c.head
	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry
}

func (c *lruCache) evict() {
	if c.tail == nil {
		return
	}

	delete(c.items, c.tail.key)
	if c.tail.prev != nil {
		c.tail.prev.next = nil
		c.tail = c.tail.prev
	} else {
		c.head = nil
		c.tail = nil
	}
}
