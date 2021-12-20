package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"time"
)

var TotalMessagesConsumed int64

type Consumer struct {
	cg sarama.ConsumerGroup
	h  *handler
}

func NewConsumer(brokerAddr string, group string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Interval = time.Second / 2

	cg, err := sarama.NewConsumerGroup([]string{brokerAddr}, group, config)
	if err != nil {
		return nil, errors.Wrap(err, "count not create consumer group")
	}
	go func() {
		for err := range cg.Errors() {
			log.Printf("consumer error: %s", err.Error())
		}
	}()
	return &Consumer{
		cg: cg,
		h:  &handler{},
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, topics ...string) error {
	c.h = &handler{
		ready: make(chan struct{}),
	}

	go func() {
		for {
			if err := c.cg.Consume(ctx, topics, c.h); err != nil {
				log.Fatalf("cannot start consumer for topics: %q. error: %s", topics, err.Error())
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-c.h.ready

	return nil
}

type handler struct {
	ready chan struct{}
}

func (c *handler) Setup(_ sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				break
			}
			session.MarkMessage(message, "")
			atomic.AddInt64(&TotalMessagesConsumed, 1)
		case <-session.Context().Done():
			log.Printf("consumer handler stopped. topic: %s. partition: %d", claim.Topic(), claim.Partition())
			return nil
		}
	}
}
