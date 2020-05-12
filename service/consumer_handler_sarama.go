package service

import (
	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"sync"
)

type consumerHandlerSarama struct {
	pool        WorkerPool
	startedChan chan struct{}
	startedOnce sync.Once
}

func (h *consumerHandlerSarama) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandlerSarama) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 處理收到的訊息, 執行次數 = 分到的partition數量
func (h *consumerHandlerSarama) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.started()
	h.pool.setConsumerGroupSession(sess)
	h.processMessages(claim) // block
	core.Logger.Debug("Message processed, partition = ", claim.Partition())
	return nil
}

func (h *consumerHandlerSarama) processMessages(claim sarama.ConsumerGroupClaim) {
	core.Logger.Debugf("Claiming message partition [%v]", claim.Partition())
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
	core.Logger.Debugf("Claimed message partition [%v]", claim.Partition())
}

func (h *consumerHandlerSarama) started() {
	h.startedOnce.Do(func() {
		h.startedChan <- struct{}{}
	})
}
