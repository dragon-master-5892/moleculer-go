package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

type RedisTransporter struct {
	prefix        string
	client        *redis.Client
	logger        *log.Entry
	serializer    serializer.Serializer
	subscriptions []redisSubscription
	ctx           context.Context
}

type RedisOptions struct {
	Address    string
	Password   string
	DB         int
	Logger     *log.Entry
	Serializer serializer.Serializer
}

type redisSubscription struct {
	topic    string
	cancelFn context.CancelFunc
}

// CreateRedisTransporter creates a Redis-based transporter.
func CreateRedisTransporter(options RedisOptions) transit.Transport {
	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})

	return &RedisTransporter{
		client:        client,
		logger:        options.Logger,
		serializer:    options.Serializer,
		subscriptions: []redisSubscription{},
		ctx:           context.Background(),
	}
}

func (t *RedisTransporter) Connect(registry moleculer.Registry) chan error {
	endChan := make(chan error)
	go func() {
		_, err := t.client.Ping(t.ctx).Result()
		if err != nil {
			t.logger.Error("Redis Connect() - Error: ", err)
			endChan <- errors.New("Error connecting to Redis: " + err.Error())
			return
		}
		t.logger.Info("Connected to Redis")
		endChan <- nil
	}()
	return endChan
}

func (t *RedisTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		for _, sub := range t.subscriptions {
			sub.cancelFn()
		}
		t.client.Close()
		t.client = nil
		endChan <- nil
	}()
	return endChan
}

func (t *RedisTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}

func (t *RedisTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if t.client == nil {
		msg := fmt.Sprint("redis.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)
	ctx, cancelFn := context.WithCancel(t.ctx)

	t.subscriptions = append(t.subscriptions, redisSubscription{topic: topic, cancelFn: cancelFn})

	go func() {
		pubsub := t.client.Subscribe(ctx, topic)
		ch := pubsub.Channel()
		for msg := range ch {
			bytes := []byte(msg.Payload)
			payload := t.serializer.BytesToPayload(&bytes)
			t.logger.Debug(fmt.Sprintf("Incoming %s packet from '%s'", topic, payload.Get("sender").String()))
			handler(payload)
		}
	}()
}

func (t *RedisTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if t.client == nil {
		msg := fmt.Sprint("redis.Publish() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)
	t.logger.Debug("redis.Publish() command: ", command, " topic: ", topic, " nodeID: ", nodeID)
	t.logger.Trace("message: \n", message, "\n - end")
	err := t.client.Publish(t.ctx, topic, t.serializer.PayloadToBytes(message)).Err()
	if err != nil {
		t.logger.Error("Error on publish: error: ", err, " command: ", command, " topic: ", topic)
		panic(err)
	}
}

func (t *RedisTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

func (t *RedisTransporter) SetNodeID(nodeID string) {
}

func (t *RedisTransporter) SetSerializer(serializer serializer.Serializer) {
	t.serializer = serializer
}
