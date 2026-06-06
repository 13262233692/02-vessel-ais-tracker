package geospatial

import "math"

type Point struct {
	Lng float64
	Lat float64
}

type LineSegment struct {
	Start Point
	End   Point
}

type Polygon struct {
	Vertices []Point
	MinLng   float64
	MaxLng   float64
	MinLat   float64
	MaxLat   float64
}

func NewPolygon(vertices []Point) *Polygon {
	if len(vertices) < 3 {
		return nil
	}

	p := &Polygon{
		Vertices: vertices,
		MinLng:   math.MaxFloat64,
		MaxLng:   -math.MaxFloat64,
		MinLat:   math.MaxFloat64,
		MaxLat:   -math.MaxFloat64,
	}

	for _, v := range vertices {
		if v.Lng < p.MinLng {
			p.MinLng = v.Lng
		}
		if v.Lng > p.MaxLng {
			p.MaxLng = v.Lng
		}
		if v.Lat < p.MinLat {
			p.MinLat = v.Lat
		}
		if v.Lat > p.MaxLat {
			p.MaxLat = v.Lat
		}
	}

	return p
}

func (p *Polygon) BBoxContains(pt Point) bool {
	return pt.Lng >= p.MinLng && pt.Lng <= p.MaxLng &&
		pt.Lat >= p.MinLat && pt.Lat <= p.MaxLat
}

func (p *Polygon) Contains(pt Point) bool {
	if !p.BBoxContains(pt) {
		return false
	}

	return rayCasting(pt, p.Vertices)
}

func rayCasting(pt Point, vertices []Point) bool {
	n := len(vertices)
	inside := false

	for i := 0; i < n; i++ {
		j := (i + 1) % n
		vi := vertices[i]
		vj := vertices[j]

		if ((vi.Lat > pt.Lat) != (vj.Lat > pt.Lat)) &&
			(pt.Lng < (vj.Lng-vi.Lng)*(pt.Lat-vi.Lat)/(vj.Lat-vi.Lat)+vi.Lng) {
			inside = !inside
		}
	}

	return inside
}

func (p *Polygon) IntersectsSegment(seg LineSegment) bool {
	if !bboxIntersectsSegment(p.MinLng, p.MaxLng, p.MinLat, p.MaxLat, seg) {
		return false
	}

	n := len(p.Vertices)
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		edge := LineSegment{Start: p.Vertices[i], End: p.Vertices[j]}
		if segmentsIntersect(edge, seg) {
			return true
		}
	}

	if p.Contains(seg.Start) || p.Contains(seg.End) {
		return true
	}

	return false
}

func bboxIntersectsSegment(minLng, maxLng, minLat, maxLat float64, seg LineSegment) bool {
	segMinLng := math.Min(seg.Start.Lng, seg.End.Lng)
	segMaxLng := math.Max(seg.Start.Lng, seg.End.Lng)
	segMinLat := math.Min(seg.Start.Lat, seg.End.Lat)
	segMaxLat := math.Max(seg.Start.Lat, seg.End.Lat)

	return segMaxLng >= minLng && segMinLng <= maxLng &&
		segMaxLat >= minLat && segMinLat <= maxLat
}

func segmentsIntersect(a, b LineSegment) bool {
	d1 := direction(b.Start, b.End, a.Start)
	d2 := direction(b.Start, b.End, a.End)
	d3 := direction(a.Start, a.End, b.Start)
	d4 := direction(a.Start, a.End, b.End)

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}

	if d1 == 0 && onSegment(b.Start, b.End, a.Start) {
		return true
	}
	if d2 == 0 && onSegment(b.Start, b.End, a.End) {
		return true
	}
	if d3 == 0 && onSegment(a.Start, a.End, b.Start) {
		return true
	}
	if d4 == 0 && onSegment(a.Start, a.End, b.End) {
		return true
	}

	return false
}

func direction(p1, p2, p3 Point) float64 {
	return (p3.Lng-p1.Lng)*(p2.Lat-p1.Lat) - (p2.Lng-p1.Lng)*(p3.Lat-p1.Lat)
}

func onSegment(p1, p2, pt Point) bool {
	return pt.Lng <= math.Max(p1.Lng, p2.Lng) && pt.Lng >= math.Min(p1.Lng, p2.Lng) &&
		pt.Lat <= math.Max(p1.Lat, p2.Lat) && pt.Lat >= math.Min(p1.Lat, p2.Lat)
}

const (
	EarthRadiusKm  = 6371.0
	DegreesPerKmLng = 1.0 / 111.32
)

func ProjectPoint(origin Point, cogDegrees float64, sogKnots float64, minutes float64) Point {
	if sogKnots <= 0 {
		return origin
	}

	speedKmh := sogKnots * 1.852
	distanceKm := speedKmh * (minutes / 60.0)

	cogRadians := cogDegrees * math.Pi / 180.0

	latKmPerDegree := 111.0
	dLat := distanceKm * math.Cos(cogRadians) / latKmPerDegree

	avgLat := (origin.Lat + origin.Lat + dLat) / 2.0
	lngKmPerDegree := 111.32 * math.Cos(avgLat*math.Pi/180.0)
	dLng := distanceKm * math.Sin(cogRadians) / lngKmPerDegree

	return Point{
		Lng: origin.Lng + dLng,
		Lat: origin.Lat + dLat,
	}
}

func HaversineDistance(a, b Point) float64 {
	lat1 := a.Lat * math.Pi / 180.0
	lat2 := b.Lat * math.Pi / 180.0
	dLat := (b.Lat - a.Lat) * math.Pi / 180.0
	dLng := (b.Lng - a.Lng) * math.Pi / 180.0

	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(dLng/2)*math.Sin(dLng/2)

	return 2 * EarthRadiusKm * math.Asin(math.Sqrt(h))
}
