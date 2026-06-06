package parser

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vessel-ais-tracker/internal/model"
)

type Config struct {
	WorkerCount int
}

type Parser struct {
	config         Config
	input          <-chan *model.AISMessage
	output         chan<- *model.ParsedMessage
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	messagesParsed uint64
	messagesFailed uint64
	messagesDropped uint64
	lastParseTime  int64
}

type ParserStats struct {
	MessagesParsed  uint64
	MessagesFailed  uint64
	MessagesDropped uint64
	LastParseTime   time.Time
}

const (
	asciiOffset = 48
	sixBitMask  = 0x3F
)

func New(config Config, input <-chan *model.AISMessage, output chan<- *model.ParsedMessage) *Parser {
	ctx, cancel := context.WithCancel(context.Background())
	return &Parser{
		config: config,
		input:  input,
		output: output,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *Parser) Start() {
	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Parser) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.input:
			if !ok {
				return
			}
			p.parseMessage(msg)
		}
	}
}

func (p *Parser) parseMessage(msg *model.AISMessage) {
	parsed := &model.ParsedMessage{
		Source: msg.Raw,
	}

	data, err := p.parseNMEA(msg.Raw)
	if err != nil {
		parsed.Error = err
		atomic.AddUint64(&p.messagesFailed, 1)
		return
	}

	parsed.Data = data
	data.Timestamp = msg.Timestamp
	atomic.AddUint64(&p.messagesParsed, 1)
	atomic.StoreInt64(&p.lastParseTime, time.Now().UTC().UnixNano())

	select {
	case p.output <- parsed:
	default:
		atomic.AddUint64(&p.messagesDropped, 1)
	}
}

func (p *Parser) parseNMEA(line string) (*model.VesselData, error) {
	parts := strings.Split(line, ",")
	if len(parts) < 7 {
		return nil, fmt.Errorf("invalid NMEA format: insufficient fields")
	}

	fragmentCount, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid fragment count: %w", err)
	}
	if fragmentCount != 1 {
		return nil, fmt.Errorf("multi-fragment messages not supported")
	}

	payload := parts[5]
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}

	bits := decodeArmored(payload)
	if len(bits) < 168 {
		return nil, fmt.Errorf("payload too short: need at least 168 bits, got %d", len(bits))
	}

	return p.decodePositionReport(bits)
}

func decodeArmored(payload string) []byte {
	byteCount := len(payload)
	bits := make([]byte, byteCount*6)

	for i, c := range payload {
		value := byte(c) - asciiOffset
		if value > 63 {
			value -= 8
		}

		bitOffset := i * 6
		bits[bitOffset+0] = (value >> 5) & 1
		bits[bitOffset+1] = (value >> 4) & 1
		bits[bitOffset+2] = (value >> 3) & 1
		bits[bitOffset+3] = (value >> 2) & 1
		bits[bitOffset+4] = (value >> 1) & 1
		bits[bitOffset+5] = value & 1
	}

	return bits
}

func getUint(bits []byte, start, length int) uint32 {
	var result uint32
	for i := 0; i < length; i++ {
		if start+i >= len(bits) {
			break
		}
		result = (result << 1) | uint32(bits[start+i])
	}
	return result
}

func getInt(bits []byte, start, length int) int32 {
	unsigned := getUint(bits, start, length)
	if unsigned&(1<<(length-1)) != 0 {
		return int32(unsigned) - int32(1<<length)
	}
	return int32(unsigned)
}

func (p *Parser) decodePositionReport(bits []byte) (*model.VesselData, error) {
	messageType := uint8(getUint(bits, 0, 6))
	if messageType < 1 || messageType > 3 {
		return nil, fmt.Errorf("unsupported message type: %d", messageType)
	}

	mmsi := getUint(bits, 8, 30)
	if mmsi == 0 {
		return nil, fmt.Errorf("invalid MMSI")
	}

	sogRaw := getUint(bits, 50, 10)
	sog := float64(sogRaw) / 10.0

	longitudeRaw := getInt(bits, 61, 28)
	longitude := float64(longitudeRaw) / 600000.0

	latitudeRaw := getInt(bits, 89, 27)
	latitude := float64(latitudeRaw) / 600000.0

	cogRaw := getUint(bits, 116, 12)
	cog := float64(cogRaw) / 10.0

	if latitude > 90 || latitude < -90 {
		return nil, fmt.Errorf("invalid latitude: %f", latitude)
	}
	if longitude > 180 || longitude < -180 {
		return nil, fmt.Errorf("invalid longitude: %f", longitude)
	}
	if cog > 360 {
		return nil, fmt.Errorf("invalid COG: %f", cog)
	}

	return &model.VesselData{
		MMSI:        mmsi,
		Latitude:    latitude,
		Longitude:   longitude,
		COG:         cog,
		SOG:         sog,
		MessageType: messageType,
	}, nil
}

func (p *Parser) GetStats() ParserStats {
	return ParserStats{
		MessagesParsed:  atomic.LoadUint64(&p.messagesParsed),
		MessagesFailed:  atomic.LoadUint64(&p.messagesFailed),
		MessagesDropped: atomic.LoadUint64(&p.messagesDropped),
		LastParseTime:   time.Unix(0, atomic.LoadInt64(&p.lastParseTime)),
	}
}

func (p *Parser) Stop() {
	p.cancel()
	p.wg.Wait()
}
