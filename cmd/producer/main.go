package main

import (
	"context"
	"flag"
	"github.com/gotidy/ptr"
	"github.com/texazcowboy/sarama-kafka-perf/internal/common"
	"github.com/texazcowboy/sarama-kafka-perf/internal/producer"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var TotalMessagesProduced int64

type topicMode string

const (
	modeSingleTopic topicMode = "single"
	modeMultiTopic  topicMode = "multi"
)

func main() {
	tMode := flag.String("tMode", "", "define topic mode. [single, multi]")
	tNum := flag.Int("tNum", 10, "define number of topics (for multi mode)")
	pNum := flag.Int("pNum", 10, "define number of partitions (for single mode)")
	tFilePath := flag.String("tFilePath", "topics.txt", "file to output created topic names")
	flag.Parse()

	brokerAddr := "127.0.0.1:9092"

	b, err := producer.NewBroker(brokerAddr)
	if err != nil {
		log.Fatalf("could not create broker. error: %s", err.Error())
	}
	defer func() {
		if err := b.Close(); err != nil {
			log.Fatalf("could not close kafka broker. error: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	p, err := producer.NewProducer(ctx, brokerAddr)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Fatalf("could not close kafka producer. error: %s", err.Error())
		}
	}()

	go velocityPrinter(ctx)

	switch topicMode(*tMode) {
	case modeSingleTopic:
		log.Printf("starting producer. topic mode: %s, partitions: %d", ptr.ToString(tMode), ptr.ToInt(pNum))
		topic := processSingleTopicMode(ctx, b, p, ptr.ToInt(pNum))
		common.WriteLinesToFile(ptr.ToString(tFilePath), topic)
	case modeMultiTopic:
		log.Printf("starting producer. topic mode: %s, topics: %d", ptr.ToString(tMode), ptr.ToInt(tNum))
		topics := processMultiTopicMode(ctx, b, p, ptr.ToInt(tNum))
		common.WriteLinesToFile(ptr.ToString(tFilePath), topics...)
	default:
		log.Print("process mode not provided")
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-sigCh

	log.Print("produce finished")
	cancel()

	time.Sleep(time.Second * 5)
}

func velocityPrinter(ctx context.Context) {
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Print("velocity printer stopped")
			return
		case <-time.After(time.Second * 5): // wait for 5 sec
		}

		timeElapsed := time.Since(start)
		mps := float64(atomic.LoadInt64(&TotalMessagesProduced)) / timeElapsed.Seconds()
		log.Printf("time elapsed: %s. produce velocity: %.4f msg/s", timeElapsed, mps)
	}
}

func processSingleTopicMode(ctx context.Context, b *producer.Broker, p *producer.Producer, pNum int) string {
	topic, err := b.CreateTopic(pNum)
	if err != nil {
		log.Fatal(err.Error())
	}
	for i := 0; i < pNum; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Print("producer routine stopped")
					return
				// case <-time.After(time.Microsecond):
				default:
				}
				p.ProduceMessageAsync(topic)
				atomic.AddInt64(&TotalMessagesProduced, 1)
			}
		}()
	}
	return topic
}

func processMultiTopicMode(ctx context.Context, b *producer.Broker, p *producer.Producer, tNum int) []string {
	topics, err := b.CreateTopics(tNum)
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, topic := range topics {
		go func(t string) {
			for {
				select {
				case <-ctx.Done():
					log.Print("producer routine stopped")
					return
				// case <-time.After(time.Microsecond):
				default:
				}
				p.ProduceMessageAsync(t)
				atomic.AddInt64(&TotalMessagesProduced, 1)
			}
		}(topic)
	}
	return topics
}
