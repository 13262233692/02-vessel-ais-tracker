package warning

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultChannel = "ais:alerts:critical"
)

type RedisPublisher struct {
	client      *redis.Client
	channel     string
	input       <-chan *Alert
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	published   uint64
	failed      uint64
	dropped     uint64
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
	Channel  string
}

func NewRedisPublisher(config RedisConfig, input <-chan *Alert) *RedisPublisher {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Channel == "" {
		config.Channel = DefaultChannel
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Address,
		Password: config.Password,
		DB:       config.DB,
	})

	return &RedisPublisher{
		client:  client,
		channel: config.Channel,
		input:   input,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (rp *RedisPublisher) Start() {
	rp.wg.Add(1)
	go rp.publishLoop()
}

func (rp *RedisPublisher) publishLoop() {
	defer rp.wg.Done()

	for {
		select {
		case <-rp.ctx.Done():
			return
		case alert, ok := <-rp.input:
			if !ok {
				return
			}
			rp.publishAlert(alert)
		}
	}
}

func (rp *RedisPublisher) publishAlert(alert *Alert) {
	payload, err := json.Marshal(alert)
	if err != nil {
		atomic.AddUint64(&rp.failed, 1)
		return
	}

	ctx, cancel := context.WithTimeout(rp.ctx, 2*time.Second)
	defer cancel()

	if err := rp.client.Publish(ctx, rp.channel, payload).Err(); err != nil {
		atomic.AddUint64(&rp.failed, 1)
		return
	}

	atomic.AddUint64(&rp.published, 1)
}

func (rp *RedisPublisher) GetStats() (published uint64, failed uint64, dropped uint64) {
	return atomic.LoadUint64(&rp.published),
		atomic.LoadUint64(&rp.failed),
		atomic.LoadUint64(&rp.dropped)
}

func (rp *RedisPublisher) Ping() error {
	ctx, cancel := context.WithTimeout(rp.ctx, 2*time.Second)
	defer cancel()
	return rp.client.Ping(ctx).Err()
}

func (rp *RedisPublisher) Stop() error {
	rp.cancel()
	rp.wg.Wait()
	if rp.client != nil {
		return rp.client.Close()
	}
	return nil
}

type LogPublisher struct {
	input     <-chan *Alert
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	published uint64
}

func NewLogPublisher(input <-chan *Alert) *LogPublisher {
	ctx, cancel := context.WithCancel(context.Background())
	return &LogPublisher{
		input:  input,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (lp *LogPublisher) Start() {
	lp.wg.Add(1)
	go lp.logLoop()
}

func (lp *LogPublisher) logLoop() {
	defer lp.wg.Done()

	for {
		select {
		case <-lp.ctx.Done():
			return
		case alert, ok := <-lp.input:
			if !ok {
				return
			}
			lp.logAlert(alert)
		}
	}
}

func (lp *LogPublisher) logAlert(alert *Alert) {
	fmt.Printf("\n🚨 ALERT: Vessel %d entering %s zone '%s' (Priority: %d)\n",
		alert.VesselMMSI, alert.FenceType, alert.FenceName, alert.Priority)
	fmt.Printf("   Position: %.6f, %.6f | COG: %.1f° | SOG: %.1f knots\n",
		alert.Latitude, alert.Longitude, alert.COG, alert.SOG)
	fmt.Printf("   ETA at zone: %s\n", alert.ETA.Format("15:04:05"))
	atomic.AddUint64(&lp.published, 1)
}

func (lp *LogPublisher) GetStats() uint64 {
	return atomic.LoadUint64(&lp.published)
}

func (lp *LogPublisher) Stop() {
	lp.cancel()
	lp.wg.Wait()
}
