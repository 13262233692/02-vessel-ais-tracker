package geospatial

import (
	"math"
	"sync"
)

const (
	DefaultGridCellSize = 0.5
)

type GridIndex struct {
	cellSize    float64
	cells       map[int64][]*Polygon
	bboxPolygons []*Polygon
	mu          sync.RWMutex
}

func NewGridIndex(cellSize float64) *GridIndex {
	if cellSize <= 0 {
		cellSize = DefaultGridCellSize
	}
	return &GridIndex{
		cellSize: cellSize,
		cells:    make(map[int64][]*Polygon),
	}
}

func (g *GridIndex) AddPolygon(p *Polygon) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.bboxPolygons = append(g.bboxPolygons, p)

	minX := int64(math.Floor(p.MinLng / g.cellSize))
	maxX := int64(math.Floor(p.MaxLng / g.cellSize))
	minY := int64(math.Floor(p.MinLat / g.cellSize))
	maxY := int64(math.Floor(p.MaxLat / g.cellSize))

	for x := minX; x <= maxX; x++ {
		for y := minY; y <= maxY; y++ {
			key := (x << 32) | (y & 0xFFFFFFFF)
			g.cells[key] = append(g.cells[key], p)
		}
	}
}

func (g *GridIndex) QueryCandidates(seg LineSegment) []*Polygon {
	g.mu.RLock()
	defer g.mu.RUnlock()

	segMinLng := math.Min(seg.Start.Lng, seg.End.Lng)
	segMaxLng := math.Max(seg.Start.Lng, seg.End.Lng)
	segMinLat := math.Min(seg.Start.Lat, seg.End.Lat)
	segMaxLat := math.Max(seg.Start.Lat, seg.End.Lat)

	minX := int64(math.Floor(segMinLng / g.cellSize))
	maxX := int64(math.Floor(segMaxLng / g.cellSize))
	minY := int64(math.Floor(segMinLat / g.cellSize))
	maxY := int64(math.Floor(segMaxLat / g.cellSize))

	candidateSet := make(map[*Polygon]struct{})

	for x := minX; x <= maxX; x++ {
		for y := minY; y <= maxY; y++ {
			key := (x << 32) | (y & 0xFFFFFFFF)
			if polygons, ok := g.cells[key]; ok {
				for _, p := range polygons {
					candidateSet[p] = struct{}{}
				}
			}
		}
	}

	candidates := make([]*Polygon, 0, len(candidateSet))
	for p := range candidateSet {
		candidates = append(candidates, p)
	}

	return candidates
}

func (g *GridIndex) QueryPointCandidates(pt Point) []*Polygon {
	g.mu.RLock()
	defer g.mu.RUnlock()

	x := int64(math.Floor(pt.Lng / g.cellSize))
	y := int64(math.Floor(pt.Lat / g.cellSize))
	key := (x << 32) | (y & 0xFFFFFFFF)

	if polygons, ok := g.cells[key]; ok {
		result := make([]*Polygon, len(polygons))
		copy(result, polygons)
		return result
	}

	return nil
}

func (g *GridIndex) Clear() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cells = make(map[int64][]*Polygon)
	g.bboxPolygons = nil
}

func (g *GridIndex) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.bboxPolygons)
}
