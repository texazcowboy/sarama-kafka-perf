package producer

import (
	"github.com/Shopify/sarama"
	"github.com/texazcowboy/sarama-kafka-perf/internal/common"
)

const tNameLength = 10

func getTopicNames(m map[string]*sarama.TopicDetail) []string {
	result := make([]string, 0, len(m))
	for key := range m {
		result = append(result, key)
	}
	return result
}

func generateTopic(pNum int32) (string, *sarama.TopicDetail) {
	return common.GenerateRandomString(tNameLength),
		&sarama.TopicDetail{
			NumPartitions:     pNum,
			ReplicationFactor: 1,
		}
}
