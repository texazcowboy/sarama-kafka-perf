package producer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"log"
	"time"
)

type Producer struct {
	ap sarama.AsyncProducer
}

func NewProducer(ctx context.Context, brokerAddr string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Flush.Frequency = time.Second

	producer, err := sarama.NewAsyncProducer([]string{brokerAddr}, config)
	if err != nil {
		return nil, errors.Wrap(err, "could not create kafka producer with default config")
	}

	go func() {
		for {
			select {
			case err, ok := <-producer.Errors():
				if !ok {
					return
				}
				log.Printf("could not produce message. error: %s", err.Error())
			case <-ctx.Done():
				return
			}
		}
	}()
	return &Producer{
		ap: producer,
	}, nil
}

func (p *Producer) ProduceMessageAsync(topic string) {
	p.ap.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("dummy message"),
	}
}

func (p *Producer) Close() error {
	return p.ap.Close()
}