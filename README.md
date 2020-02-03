# dispatcher

利用簡單的介面, 透過kafaka調度任務及推送消息

參考 [producer](./examples/producer/main.go), [consumer](./examples/consumer/main.go) 使用範例

## 建置
`go get gitlab.paradise-soft.com.tw/glob/dispatcher`
Windows環境需安裝 [GCC](./build/mingw-w64-install.exe) , Architecure選x86_64, 環境變數path增加 `{{GCC安裝路徑}}\bin`

## Quick Start
### 初始化
在使用其他方法前, 須先完成初始化
```go
dispatcher.Init(brokers, ...opts)
```
- `brokers`: `[]string`, kafka機器ip清單, ex. ["1.0.0.1:000", "2.0.0.1:000"]
- `opts`:
  - `InitSetKafkaConfig`: 指定各Kafka相關設定, 包含partition數量, replica數量, 訊息大小...等
  - `InitSetDefaultGroupID`: 預設GroupID, 將用於producer & consumer 接收訊息時的群組編號, 未指定將設為hash過的mac address
  - `InitSetLogLevel`: 指定log等級, 可指定 "info" (預設), "debug". 設為debug時將開啟sarama的log
  - `InitEnableSaramaLog`: 啟用sarama log
  - `InitSetMonitorHost`: 設定monitor host, 供GetConsumeStatusByGroupID()使用

### 傳送訊息

```go
dispatcher.Send(topic, message, ...opts) error
```
- `topic`: `string`, 訊息queue的名稱, 監聽時指定相同topic以取得訊息
- `message`: `[]byte`, 要傳送的訊息
- `opts`:
  - `ProducerEnsureOrder`: 確保接收時訊息有序, dispatcher會將message的key設為固定值(topic), 讓訊息進到同個partition, 未指定時隨機分配partition
  - `ProducerSetMessageKey`: 使用者自訂每個訊息的key值, 採用順序: Key > EnsureOrder
  - `ProducerAddErrHandler`: 接收consumer的callback error
  - `ProducerCollectReplyMessage`: 接收consumer的回條訊息, 包含訊息編號, 處理過程的時間戳, consumer group id等資訊. 等待將在達到指定的timeout後回傳error.

### 接收訊息

#### 單次接收
指定topic, 使用callback方法來處理訊息

```go
dispatcher.Subscribe(topic, callback, ...opts) (SubscriberCtrl, error)
```
- `callback`: `func (value []byte) error`, 處理訊息
- `SubscriberCtrl`: 包含 `Errors()`接收監聽過程error, `Stops()`手動終止監聽等控制功能
- `error`: 建立subscribe error
- `opts`:
  - `ConsumerSetGroupID`: 指定consumer group id, 本次consume將覆蓋InitDefaultGroupID
  - `ConsumerSetAsyncNum`: 指定同時處理訊息的goroutine數量
  - `ConsumerOmitOldMsg`: 是否忽略舊訊息, 注意: *僅在該group初次監聽topic有效*
  - `ConsumerNotCommitOnError`: callback發生error時, 不commit該筆訊息的offset(不記錄該筆訊息已消費)

#### 斷線重試接收
```go
dispatcher.SubscribeWithRetry(topic, callback, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100)) (ctrl *SubscriberWithRetryCtrl)
```
- `topic`, `callback`, `opts`: 同 subscribe
- `failRetryLimit` 重試次數上限, 超過時取消監聽, 並回傳最後一個發生的error
- `getRetryDuration` 依照失敗次數回傳每次重試需等待時間 (`func(failCount int) time.Duration`)
- `SubscriberWithRetryCtrl`: 包含
  - `Errors()` 接收監聽過程error
  - `Stops()` 手動終止監聽
  - `GroupID` 實際監聽使用的consumer group id
- `opts`: 同Subscribe的參數

### 監聽消費狀況
```go
dispatcher.GetConsumeStatusByGroupID(_topic, _groupID) (status ConsumeStatus, err error)
````
`ConsumeStatus` 包含 `GroupID` 及 `LagCount`

### 可用選項

各API提供可選設定做為非必要參數, 所有可用的選項參見 [options.go](./options.go)

