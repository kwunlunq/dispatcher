package dispatcher

import (
	"context"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type ConsumerCallback func(key, value string)

func Subscribe(topic string, groupID string, callback ConsumerCallback) {
	// Start with a client
	client, err := sarama.NewClient(brokerList, saramaConfig)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	log.Printf("Listening topic [%v] with groupID [%v] ...\n", topic, groupID)

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{topic}
		handler := commonConsumerGroupHandler{callback}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

type commonConsumerGroupHandler struct {
	callback ConsumerCallback
}

func (commonConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (commonConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h commonConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		h.callback(string(msg.Key[:]), string(msg.Value[:]))
		sess.MarkMessage(msg, "")
	}
	return nil
}
