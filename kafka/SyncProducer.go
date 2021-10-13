package kafka

import (
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
)

type Producer struct {
	client          sarama.SyncProducer
	judingToolTopic string
}

func NewSyncProducer(brokers []string, topic string) *Producer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_0_0_0
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		panic(err)
	}
	return &Producer{client: producer, judingToolTopic: os.Getenv("JUDING_TOOL_TOPIC")}
}

func (p *Producer) Start(tokenResult chan *TokenResult) (func(), func()) {
	return func() {
		for msg := range tokenResult {
			d, _ := json.Marshal(msg)
			msg := &sarama.ProducerMessage{
				Value: sarama.ByteEncoder(d),
				Topic: p.judingToolTopic,
				Key:   sarama.StringEncoder(teamName),
			}
			p.client.SendMessage(msg)
		}
	}, func() { p.client.Close() }
}
