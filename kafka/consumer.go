package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	teamName    = os.Getenv("TEAM_NAME")
	groupID     = fmt.Sprintf("%s-consumer-group", teamName)
	JsonMsgChan = make(chan *JsonMsg, 128)
)

type CompareResult struct {
	SmallId uint64 `json:"SmallId"`
	LargeId uint64 `json:"LargeId"`
	Key     string `json:"Key"`
}

type JsonMsg struct {
	Key   string      `json:"key,omitempty"`
	Id    uint64      `json:"id"`
	Value interface{} `json:"value"`
}

type HandlerDataInput struct {
	smallMsg   map[string]uint64
	largeMsg   map[string]struct{}
	largeMsgId uint64
	count      uint
}

type ConsumerGroupHandler struct {
	consumerGroup  sarama.ConsumerGroup
	messageHandler *MessageHandler
}

func NewConsumerHandler(addrs []string, compareResultChan chan *CompareResult) *ConsumerGroupHandler {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Group.Heartbeat.Interval = 1000 * time.Millisecond
	config.Consumer.Group.Rebalance.Retry.Backoff = 1000 * time.Millisecond
	config.Metadata.Full = true
	consumer, err := sarama.NewConsumerGroup(addrs, groupID, config)
	if err != nil {
		log.Panicf("Error creating consumer group: %v", err)
	}
	return &ConsumerGroupHandler{
		consumerGroup:  consumer,
		messageHandler: NewMessageHandler(compareResultChan),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// receiveMsg := 0
	for msg := range claim.Messages() {
		// receiveMsg++
		m := JsonMsg{}
		_ = json.Unmarshal(msg.Value, &m)
		m.Key = string(msg.Key)
		JsonMsgChan <- &m
		// fmt.Printf("receive %v message\n", receiveMsg)
	}
	return nil
}

func (h *ConsumerGroupHandler) Start(ctx context.Context, topic string) (func(), func()) {
	go func() {
		for {
			if err := h.consumerGroup.Consume(ctx, strings.Split(topic, ","), h); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return h.messageHandler.FindMatch, func() {
		h.consumerGroup.Close()
	}
}

type MessageHandler struct {
	dataInputMap map[string]*HandlerDataInput
	outputChan   chan *CompareResult
}

func NewMessageHandler(compareResultChan chan *CompareResult) *MessageHandler {
	return &MessageHandler{
		dataInputMap: make(map[string]*HandlerDataInput, 500),
		outputChan:   compareResultChan,
	}
}

func (h *MessageHandler) FindMatch() {
	for msg := range JsonMsgChan {
		key := msg.Key
		id := msg.Id
		value := msg.Value
		if h.dataInputMap[key] == nil {
			h.dataInputMap[key] = &HandlerDataInput{
				smallMsg: make(map[string]uint64, 127),
				largeMsg: make(map[string]struct{}, 128),
				count:    0,
			}
		}
		dataInput := h.dataInputMap[key]
		if v, ok := value.(string); ok {
			dataInput.smallMsg[v] = id
		} else {
			for _, data := range value.([]interface{}) {
				dataInput.largeMsg[data.(string)] = struct{}{}
				dataInput.largeMsgId = id
			}
		}
		dataInput.count++
		if dataInput.count == 128 {
			for largeValue := range dataInput.largeMsg {
				if smallMsgId, exist := dataInput.smallMsg[largeValue]; exist {
					h.outputChan <- &CompareResult{
						SmallId: smallMsgId,
						LargeId: dataInput.largeMsgId,
						Key:     key,
					}
					break
				}
			}
			delete(h.dataInputMap, key)
		}
	}
}
