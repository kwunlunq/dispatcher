package model

import "time"

type Message struct {
	TaskID               string // 任務ID, 訊息編號
	Topic                string
	Key                  string
	Value                []byte
	Partition            int32
	Offset               int64
	ConsumerErrorStr     string // Consumer執行callback時發生error後回傳的錯誤
	ProducerSentTime     time.Time
	ConsumerReceivedTime time.Time
	ConsumerFinishTime   time.Time
	ProducerReceivedTime time.Time // Producer收到由consumer發送回來的訊息的時間 (error msg, reply msg)
	IsReplyMessage       bool      // 是否回送回條, 由producer設定, consumer發送前設為false
	IsSendError          bool      // 是否回送callback error, 由producer設定, consumer發送前設為false
}
