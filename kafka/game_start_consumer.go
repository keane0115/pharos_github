package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type GameStartConsumer struct {
	gameStartTopic string
	consumer       sarama.Consumer
}

func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V1_0_0_0
	return config
}

func NewGameStartConsumer(addrs []string, topic string) *GameStartConsumer {
	consumer, err := sarama.NewConsumer(addrs, newConfig())
	if err != nil {
		panic(err)
	}
	return &GameStartConsumer{
		gameStartTopic: topic,
		consumer:       consumer,
	}
}

func (c *GameStartConsumer) InitGameConsumerTopic(ctx context.Context, topic chan string) {
	partitionList, err := c.consumer.Partitions(c.gameStartTopic)
	if err != nil {
		log.Panicf("fail to get list of partition:err%v\n", err)
	}

	for partition := range partitionList {
		pc, err := c.consumer.ConsumePartition(c.gameStartTopic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Panicf("failed to start consumer for partition %d,err:%v\n", partition, err)
		}
		go func(sarama.PartitionConsumer) {
			defer pc.AsyncClose()
			select {
			case msg := <-pc.Messages():
				topic <- string(msg.Value)
				return
			case <-ctx.Done():
				fmt.Println("Exit with %w", ctx.Err())
				return
			}
		}(pc)
	}
}
