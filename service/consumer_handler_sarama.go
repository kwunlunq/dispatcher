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
	//go h.markProcessedMessages(sess, claim)
	h.processMessages(sess, claim) // block
	core.Logger.Debug("Message processed, partition = ", claim.Partition())
	return nil
}

func (h *consumerHandlerSarama) processMessages(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) {
	core.Logger.Debugf("Claiming message partition [%v]", claim.Partition())
	//core.Logger.Infof("Sess of partition %v: %+v", claim.Partition(), sess)
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
	core.Logger.Debugf("Claimed message partition [%v]", claim.Partition())
}

// TODO: 待整理 移除
//func (h *consumerHandlerSarama) markProcessedMessages(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) {
//	fmt.Printf("marking message of claim topic: %v, partition: %v\n", claim.Topic(), claim.Partition())
//	for {
//		select {
//		case result, ok := <-h.pool.Processed():
//			if !ok {
//				return
//			}
//			core.Logger.Debugf("Message marked: %v: %v (sess part: %v)", result.Partition, result.Offset, claim.Partition())
//			sess.MarkMessage(result, "")
//		case <-h.pool.Context().Done():
//			core.Logger.Infof("Partition %v done !!", claim.Partition())
//			return
//		}
//	}
//}

func (h *consumerHandlerSarama) started() {
	h.startedOnce.Do(func() {
		h.startedChan <- struct{}{}
	})
}
