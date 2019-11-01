# Changelog

### Version 1.11.0 (2019-11-1)

>**`重要` *此更新不影響原有接口, 但將會改變dispatcher傳送訊息的格式, 若consumer/producer無法同時更新到1.11.x, 需先將其中一個更新到1.10.13以相容新版訊息格式***

Features:
- 增加回條機制: Producer可指定接收consumer的回條訊息, 包含訊息編號, 處理過程的時間戳, consumer group id等資訊. 等待將在達到指定的timeout後回傳error.
```go
 dispatcher.Send(topic, msg, dispatcher.ProducerCollectReplyMessage(replyHandler, collectReplyTimeout))
```

### Version 1.10.13 (2019-11-1)

Improvements:
- 支援1.11.x版訊息傳輸格式(producer/consumer)

### Version 1.10.12 (2019-10-24)

Bug Fixes:
- 修復並行goroutine重複訂閱同個topic產生的異常問題

### Version 1.10.11 (2019-10-22)

Features:
- 增加producer option: `ProducerSetMessageKey` 自訂message key, 影響進入的partition

Bug Fixes:
- 修復初始化時使用者指定0值不會被覆蓋成預設值問題 (workerNum, groupID)

### Version 1.10.10 (2019-10-15)

Bug Fixes:
- 修復consumer不停中斷重連的問題 (caused by consumer group rebalancing)
- Producer/Consumer default timeout: 10s > 30s, try fix issue: 
```error while consuming Hao_DEV_Linux_SDK/2: kafka server: Request exceeded the user-specified time limit in the request```

### Version 1.10.9 (2019-09-23)

Bug Fixes:
- 修復失敗後重建topic時可能出現非預期partition數量問題

### Version 1.10.6 (2019-09-20)

Improvements:
- 為訂閱不同topic的consumer指定不同的groupID: {groupID}:{topic}

### Version 1.10.3 (2019-09-19)

Bug Fixes:
- 處理consumer異常結束可能發生close on closed channel的問題

### Version 1.10.2 (2019-09-18)

Bug Fixes:
- 暫時移除timeout相關設定的套用
- 處理幾個可能導致客戶端卡住的問題
- 增加Consume Error: ErrConsumeStopWithoutError

### Version 1.10.1 (2019-09-16)

Improvements:
- 整理Init設定Kafka相關參數: `InitSetKafkaConfig`
  - 使用方式:
  ```go
  dispatcher.Init(brokers, dispatcher.InitSetKafkaConfig(dispatcher.KafkaConfig{Timeout: 60 * time.Second}))
  ```
  - 包含欄位: 
  	- `TopicPartitionNum` 每個topic的partition
  	- `TopicReplicationNum` 同步副本數
  	- `MinInsyncReplicas` 最小同步數量
  	- `Timeout` 與Kafka溝通, Net/Produce/Consume中各操作的timeout時間
  	- `MsgMaxBytes` 訊息大小上限
- 移除參數(移到KafkaConfig): `InitSetPartitionNum`, `InitSetReplicaNum`, `InitSetDefaultGroupID`

### Version 1.10.0 (2019-09-09)

Feature:
- 增加方法`SubscribeWithRetry` 包裝過斷線重連的subscribe方法
  - `topic`, `callback`, `opts`: 同 subscribe
  - `failRetryLimit` 重試次數上限, 超過時取消監聽, 並回傳最後一個發生的error
  - `getRetryDuration` 依照失敗次數回傳每次重試需等待時間 (`func(failCount int) time.Duration`)

Improvements:
- `Subscribe` 回傳參數調整
  - `SubscriberCtrl` Subscriber控制物件: 包含方法 `Errors()`監聽過程的error chan, `Stop()`手動終止監聽 
  - `error` 建立subscriber錯誤, 成功建立時回傳nil
- Kafka失敗/手動取消訂閱時: 清空workerPool, usedTopic紀錄

Bug Fixes:
- 修復幾個會造成dispatcher在kafka發生異常後無法恢復的問題
- 嘗試解決create topic失敗-broken pipe: Topic創建時使用獨立連線   

### Version 1.9.0 (2019-08-27)

Improvements:
- Subscribe() will be blocked until any error occurs.

### Version 1.8.0 (2019-08-14)

Improvements:
- GroupID in Init is now optional, mac address will be used if not specified.
- Send/Subscribe catch most errors.

### Version 1.7.2 (2019-07-23)

Bug Fixes:
- Remove default value for topic's replication num

### Version 1.7.0 (2019-06-10)

Features:
- Introduce functional options to public APIs.
- Init() method to get necessary setting value instead of reading config file directly.
- Utilize `go.uber.org/zap` as project's logger.

Improvements:
- Add consumer option: setGroupID

### Version 1.5.2 (2019-04-29)

Improvements:
- Extend kafkat read timeout to 300s

### Version 1.5.0 (2019-04-25)

Improvements:
- Messages will now be distributed evenly to consumers in a group.
- New options in app.conf: `topic_partition_num` `topic_replication_num` `msg_max_bytes`
- Expand message size to 20M

### Version 1.4.0 (2019-04-22)

Improvements:
- New param in app.conf: [dispatcher] group_id, used by Sender's error handler & subscriber when no group-id is given.
- Change setting value of Consumer.Offsets.Initial from OffsetNewest to OffsetOldest, so missing message could be consumed.

### Version 1.3.0 (2019-04-18)

New Features:
- Func Subscribe() become SubscriberGroup(), adding new func Subscribe which no groupID is needed.
- Send() has new param: ErrCallback, to process error when something went wrong during consumer's callback.

Improvements:
- Panic on user's callback will now print complete stacktrace.

#### Version 1.2.0 (2019-04-17)

Improvments:
- Subscriber now accept nil callback param
- Establish new client for each subscriber, so multiple Producer/Subscriber is possible.
- 解除部分panic, 讓sarama接手自動重新連接. Ex.新consumer加入group導致原有consumer收到錯誤.

### Version 1.1.1 (2019-04-17)

Improvements:
- Update go.mod: yaitoo to latest

#### Version 1.1.0 (2019-04-17)

New Features:
- Create topic automatically before sending/subscribing.

Bug Fixes:
- Use shared client instead of creating one everytime, trying to fix `Failed to start Sarama producer: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`.

#### Version 1.0.0 (2019-04-15)

New Features:
- Add async ablility to Consumer.

#### Version 0.1.0 (2019-04-08)

First tagged version.

New Features:
- Consumer & Producer.