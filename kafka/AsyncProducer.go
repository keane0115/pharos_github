package kafka

import (
	"encoding/json"
	"os"

	"github.com/Shopify/sarama"
)

// AsyncProducer is a wrapper on the sarama async_producer.
type AsyncProducer struct {
	asyncProducer   sarama.AsyncProducer
	judingToolTopic string
}

// NewAsyncProducerFromEnv creates AsyncProducer using broker values from environment and default sarama configurations.
func NewAsyncProducer(brokers []string, topic string) *AsyncProducer {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.ClientID, _ = os.Hostname()
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil
	}
	return &AsyncProducer{asyncProducer: producer, judingToolTopic: os.Getenv("JUDING_TOOL_TOPIC")}
}

func (a *AsyncProducer) Start(tokenResult chan *TokenResult) (func(), func()) {
	return func() {
		for msg := range tokenResult {
			d, _ := json.Marshal(*msg)
			msg := &sarama.ProducerMessage{
				Value: sarama.ByteEncoder(d),
				Topic: a.judingToolTopic,
				Key:   sarama.StringEncoder(teamName),
			}
			a.asyncProducer.Input() <- msg
		}
	}, func() { a.asyncProducer.AsyncClose() }
}
