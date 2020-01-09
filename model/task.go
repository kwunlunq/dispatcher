package model

import (
	"github.com/google/uuid"
	"time"
)

type Task struct {
	TaskInfo
	Replier
	Message Message
}

type TaskInfo struct {
	CreatedTime     time.Time
	ExpiredTimeNano int64
}

type Replier struct {
	Handler    func(message Message, err error)
	IsExecuted bool
	//Locker           *sync.RWMutex
	//ExecutedGroupIDs map[string]struct{}
}

// TODO: 待整理
func (r Replier) HandleMessage(groupID string, message Message, err error) {
	//if r.IsGroupIDExecuted(groupID) {
	//core.Logger.Infof("GroupID executed: message: [%v], groupID: [%v]", string(message.Value), groupID)
	//return
	//}
	r.IsExecuted = true
	r.Handler(message, err)
	//r.ExecutedGroupIDs[groupID] = struct{}{}
}

//func (r Replier) IsGroupIDExecuted(groupID string) (executed bool) {
//	_, executed = r.ExecutedGroupIDs[groupID]
//	return
//}

//func (r Replier) IsExecuted() (executed bool) {
//	executed = len(r.ExecutedGroupIDs) > 0
//	return
//}

// NewTask 利用 topic, message, dispatcher設定等 包裝成dispatcher用的task物件
func NewTask(topic string, message []byte, dis Dispatcher) (task Task) {
	var messageKey string
	var expiredTimeNano int64
	if dis.ProducerReplyTimeout > 0 {
		expiredTimeNano = time.Now().Add(dis.ProducerReplyTimeout).UnixNano()
	}
	if dis.ProducerEnsureOrder {
		messageKey = topic
	}
	if dis.ProducerMessageKey != "" {
		messageKey = dis.ProducerMessageKey
	}
	task = Task{
		Message: Message{
			TaskID:           uuid.New().String(),
			Topic:            topic,
			Key:              messageKey,
			Value:            message,
			ProducerSentTime: time.Now(),
			IsSendError:      dis.ProducerErrHandler != nil,
			IsReplyMessage:   dis.ProducerReplyHandler != nil,
		},
		TaskInfo: TaskInfo{
			CreatedTime:     time.Now(),
			ExpiredTimeNano: expiredTimeNano,
		},
		Replier: Replier{
			Handler: func(message Message, err error) {
				dis.ProducerReplyHandler(message, err)
			},
			//ExecutedGroupIDs: map[string]struct{}{},
			//Locker:           &sync.RWMutex{},
		},
	}
	return
}
