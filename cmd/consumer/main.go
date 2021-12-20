package main

import (
	"context"
	"flag"
	"github.com/gotidy/ptr"
	"github.com/pkg/profile"
	"github.com/texazcowboy/sarama-kafka-perf/internal/common"
	"github.com/texazcowboy/sarama-kafka-perf/internal/consumer"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type consumerMode string

const (
	consumerSingle = "single"
	consumerMulti  = "multi"
)

type topicMode string

const (
	topicSingle topicMode = "single"
	topicMulti  topicMode = "multi"
)

func main() {
	tMode := flag.String("tMode", "", "define topic mode. [single, multi]")
	cMode := flag.String("cMode", "", "define consumer mode. [single, multi]")
	tProf := flag.String("tProf", "cpu", "define profile type. [cpu (default), mem, gor]")
	pNum := flag.Int("pNum", 10, "define number of partitions (for single mode)")
	tFilePath := flag.String("tFilePath", "topics.txt", "file to read topic names")
	flag.Parse()

	brokerAddr := "127.0.0.1:9092"
	group := "test-group" + common.GenerateRandomString(5)
	topics := common.ReadLinesFromFile(ptr.ToString(tFilePath))

	ctx, cancel := context.WithCancel(context.Background())

	profilePath := strings.Join([]string{"profiles", "tmode", ptr.ToString(tMode), "cmode", ptr.ToString(cMode)}, "/")

	switch ptr.ToString(tProf) {
	case "cpu":
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(profilePath)).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile, profile.ProfilePath(profilePath)).Stop()
	case "gor":
		defer profile.Start(profile.GoroutineProfile, profile.ProfilePath(profilePath)).Stop()
	default:
		log.Fatalf("unknown profiling mode")
	}

	go velocityPrinter(ctx)

	switch topicMode(*tMode) {
	case topicSingle:
		log.Printf("starting consumer group: %s. topic mode: %s. consumer mode: %s. topic: %q", group, ptr.ToString(tMode), ptr.ToString(cMode), topics)
		switch consumerMode(*cMode) {
		case consumerSingle:
			c, err := consumer.NewConsumer(brokerAddr, group)
			if err != nil {
				log.Fatalf("could not create consumer. error: %s", err.Error())
			}
			if err := c.Consume(ctx, topics...); err != nil {
				log.Fatalf("could not start consumer. error: %s", err.Error())
			}
		case consumerMulti:
			for i := 0; i < ptr.ToInt(pNum); i++ {
				go func() {
					c, err := consumer.NewConsumer(brokerAddr, group)
					if err != nil {
						log.Fatalf("could not create consumer. error: %s", err.Error())
					}
					if err := c.Consume(ctx, topics...); err != nil {
						log.Fatalf("could not start consumer. error: %s", err.Error())
					}
				}()
			}
		default:
			log.Fatal("consumer mode not provided")
		}
	case topicMulti:
		log.Printf("starting consumer group: %s. topic mode: %s. consumer mode: %s. topic: %q", group, ptr.ToString(tMode), ptr.ToString(cMode), topics)
		switch consumerMode(*cMode) {
		case consumerSingle:
			c, err := consumer.NewConsumer(brokerAddr, group)
			if err != nil {
				log.Fatalf("could not create consumer. error: %s", err.Error())
			}
			if err := c.Consume(ctx, topics...); err != nil {
				log.Fatalf("could not start consumer. error: %s", err.Error())
			}
		case consumerMulti:
			for _, topic := range topics {
				go func(t string) {
					c, err := consumer.NewConsumer(brokerAddr, group)
					if err != nil {
						log.Fatalf("could not create consumer. error: %s", err.Error())
					}
					if err := c.Consume(ctx, t); err != nil {
						log.Fatalf("could not start consumer. error: %s", err.Error())
					}
				}(topic)
			}
		default:
			log.Fatal("consumer mode not provided")
		}
	default:
		log.Fatal("topic mode not provided")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-sigCh

	log.Print("consume finished")
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
		mps := float64(atomic.LoadInt64(&consumer.TotalMessagesConsumed)) / timeElapsed.Seconds()
		log.Printf("time elapsed: %s. consume velocity: %.4f msg/s", timeElapsed, mps)
	}
}
