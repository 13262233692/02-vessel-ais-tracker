package geofence

import (
	"sync"

	"github.com/vessel-ais-tracker/internal/geospatial"
)

type FenceType string

const (
	TypeNoEntryZone    FenceType = "NO_ENTRY"
	TypeReefZone       FenceType = "REEF"
	TypeRestrictedZone FenceType = "RESTRICTED"
)

type Geofence struct {
	ID       string
	Name     string
	Type     FenceType
	Priority int
	Polygon  *geospatial.Polygon
}

type Manager struct {
	fences     map[string]*Geofence
	gridIndex  *geospatial.GridIndex
	mu         sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		fences:    make(map[string]*Geofence),
		gridIndex: geospatial.NewGridIndex(0.25),
	}
}

func (m *Manager) AddFence(fence *Geofence) {
	if fence == nil || fence.Polygon == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.fences[fence.ID] = fence
	m.gridIndex.AddPolygon(fence.Polygon)
}

func (m *Manager) RemoveFence(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.fences, id)
	m.rebuildIndex()
}

func (m *Manager) GetFence(id string) (*Geofence, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	f, ok := m.fences[id]
	return f, ok
}

func (m *Manager) GetAll() []*Geofence {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Geofence, 0, len(m.fences))
	for _, f := range m.fences {
		result = append(result, f)
	}
	return result
}

func (m *Manager) rebuildIndex() {
	m.gridIndex = geospatial.NewGridIndex(0.25)
	for _, f := range m.fences {
		m.gridIndex.AddPolygon(f.Polygon)
	}
}

func (m *Manager) CheckCollision(seg geospatial.LineSegment) []*Geofence {
	m.mu.RLock()
	defer m.mu.RUnlock()

	candidates := m.gridIndex.QueryCandidates(seg)
	if len(candidates) == 0 {
		return nil
	}

	polygonToFence := make(map[*geospatial.Polygon]*Geofence, len(m.fences))
	for _, f := range m.fences {
		polygonToFence[f.Polygon] = f
	}

	var collisions []*Geofence
	for _, poly := range candidates {
		if poly.IntersectsSegment(seg) {
			if fence, ok := polygonToFence[poly]; ok {
				collisions = append(collisions, fence)
			}
		}
	}

	return collisions
}

func (m *Manager) CheckPointInFence(pt geospatial.Point) []*Geofence {
	m.mu.RLock()
	defer m.mu.RUnlock()

	candidates := m.gridIndex.QueryPointCandidates(pt)
	if len(candidates) == 0 {
		return nil
	}

	polygonToFence := make(map[*geospatial.Polygon]*Geofence, len(m.fences))
	for _, f := range m.fences {
		polygonToFence[f.Polygon] = f
	}

	var inside []*Geofence
	for _, poly := range candidates {
		if poly.Contains(pt) {
			if fence, ok := polygonToFence[poly]; ok {
				inside = append(inside, fence)
			}
		}
	}

	return inside
}

func (m *Manager) FenceCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.fences)
}

func (m *Manager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fences = make(map[string]*Geofence)
	m.gridIndex.Clear()
}
