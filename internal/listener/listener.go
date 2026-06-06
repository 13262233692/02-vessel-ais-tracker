package listener

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/vessel-ais-tracker/internal/model"
)

type Config struct {
	UDPPort      int
	ReadBuffer   int
	MaxPacketSize int
}

type Listener struct {
	config     Config
	output     chan<- *model.AISMessage
	conn       *net.UDPConn
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stats      ListenerStats
	statsMu    sync.RWMutex
}

type ListenerStats struct {
	PacketsReceived uint64
	BytesReceived   uint64
	Errors          uint64
	LastPacketTime  time.Time
}

func New(config Config, output chan<- *model.AISMessage) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		config: config,
		output: output,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (l *Listener) Start() error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", l.config.UDPPort))
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}

	if l.config.ReadBuffer > 0 {
		if err := conn.SetReadBuffer(l.config.ReadBuffer); err != nil {
			return fmt.Errorf("set read buffer: %w", err)
		}
	}

	l.conn = conn
	l.wg.Add(1)
	go l.receiveLoop()

	return nil
}

func (l *Listener) receiveLoop() {
	defer l.wg.Done()

	buf := make([]byte, l.config.MaxPacketSize)

	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		n, remoteAddr, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			if l.ctx.Err() != nil {
				return
			}
			l.incrementError()
			continue
		}

		if n == 0 {
			continue
		}

		l.handlePacket(buf[:n], remoteAddr.String())
	}
}

func (l *Listener) handlePacket(data []byte, source string) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}

		if !l.isValidNMEA(line) {
			l.incrementError()
			continue
		}

		msg := &model.AISMessage{
			Raw:        line,
			Timestamp:  time.Now().UTC(),
			PacketType: l.detectPacketType(line),
		}

		select {
		case l.output <- msg:
			l.incrementStats(uint64(len(line)))
		default:
			l.incrementError()
		}
	}
}

func (l *Listener) isValidNMEA(line string) bool {
	if !strings.HasPrefix(line, "!") {
		return false
	}
	if len(line) < 10 {
		return false
	}
	starIdx := strings.LastIndex(line, "*")
	return starIdx > 0 && starIdx < len(line)-2
}

func (l *Listener) detectPacketType(line string) string {
	if strings.Contains(line, "AIVDM") {
		return "AIVDM"
	}
	if strings.Contains(line, "AIVDO") {
		return "AIVDO"
	}
	return "UNKNOWN"
}

func (l *Listener) incrementStats(bytes uint64) {
	l.statsMu.Lock()
	defer l.statsMu.Unlock()
	l.stats.PacketsReceived++
	l.stats.BytesReceived += bytes
	l.stats.LastPacketTime = time.Now().UTC()
}

func (l *Listener) incrementError() {
	l.statsMu.Lock()
	defer l.statsMu.Unlock()
	l.stats.Errors++
}

func (l *Listener) GetStats() ListenerStats {
	l.statsMu.RLock()
	defer l.statsMu.RUnlock()
	return l.stats
}

func (l *Listener) Stop() {
	l.cancel()
	if l.conn != nil {
		l.conn.Close()
	}
	l.wg.Wait()
}
