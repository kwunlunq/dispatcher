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
- `opts`: 可選參數

### 傳送訊息

```go
dispatcher.Send(topic, message, ...opts) error
```
- `topic`: `string`, 訊息queue的名稱, 監聽時指定相同topic以取得訊息
- `message`: `[]byte`, 要傳送的訊息
- `opts`:
  - `ProducerEnsureOrder`: 確保接收時訊息有序, dispatcher會將message的key設為固定值(topic), 讓訊息進到同個partition, 未指定時隨機分配partition
  - `ProducerSetMessageKey`: 使用者自訂每個訊息的key值, 採用順序: Key > EnsureOrder 

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

#### 斷線重試接收
```go
dispatcher.SubscribeWithRetry(topic, callback, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100))
```
- `topic`, `callback`, `opts`: 同 subscribe
- `failRetryLimit` 重試次數上限, 超過時取消監聽, 並回傳最後一個發生的error
- `getRetryDuration` 依照失敗次數回傳每次重試需等待時間 (`func(failCount int) time.Duration`)


### 可用選項

各API提供可選設定做為非必要參數, 所有可用的選項參見 [options.go](./options.go)

1. Init指定預設全域GroupID, 用於subscribe時的群組 及 send時的錯誤接收群組, 未指定時使用機器的 mac address
```go
dispatcher.InitSetDefaultGroupID(groupID)
```
`groupID`: `string`, 群組代號, 用以紀錄消費訊息的紀錄, 若其他機器設定相同groupID會組成群組, kafka在傳送訊息時僅送給group中的其中一人(監聽相同topic時)

1. Subscriber以多執行續處理訊息
```go
dispatcher.Subscribe(topic, callback, dispatcher.ConsumerSetAsyncNum(5))
```

2. Subscriber只接收自己啟動後的新訊息, 不從未讀過的開始讀 (已有收過訊息紀錄時無效)
```go
dispatcher.Subscribe(topic, callback, dispatcher.ConsumerOmitOldMsg())
``` 

3. Sender接收Subscriber callback回傳的錯誤
```go
dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))
```

4. Sender確保Subscriber收到訊息是和送出時的順序完全一致, 但會降低效能 (只使用一個partition)
```go
dispatcher.Send(topic, msg, dispatcher.ProducerEnsureOrder())
```

