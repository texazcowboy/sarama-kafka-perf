package producer

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"log"
	"time"
)

type Broker struct {
	sb *sarama.Broker
}

func NewBroker(addr string) (*Broker, error) {
	broker := sarama.NewBroker(addr)
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	err := broker.Open(config)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to broker")
	}
	connected, err := broker.Connected()
	if err != nil {
		return nil, errors.Wrap(err, "could check broker connection state")
	}
	log.Printf("broker connection state: %t", connected)

	return &Broker{broker}, nil
}

func (b *Broker) CreateTopic(pNum int) (string, error) {
	topics, err := b.createTopics(1, pNum)
	if err != nil {
		return "", err
	}
	return topics[0], nil
}

func (b *Broker) CreateTopics(tNum int) ([]string, error) {
	return b.createTopics(tNum, 1)
}

func (b *Broker) createTopics(tNum, pNum int) ([]string, error) {
	topicDetails := make(map[string]*sarama.TopicDetail, tNum)
	for i := 0; i < tNum; i++ {
		tName, tDetails := generateTopic(int32(pNum))
		topicDetails[tName] = tDetails
	}
	ctReq := sarama.CreateTopicsRequest{
		TopicDetails: topicDetails,
		Timeout:      time.Second * 20,
	}
	_, err := b.sb.CreateTopics(&ctReq)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create topics")
	}
	tNames := getTopicNames(topicDetails)
	log.Printf("created topics: %q", tNames)
	return tNames, nil
}

func (b *Broker) Close() error {
	return b.sb.Close()
}
