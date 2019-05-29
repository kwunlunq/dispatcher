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
dispatcher.Init(brokers, groupID)
```
`brokers`: `[]string`, kafka機器ip清單, ex. ["1.0.0.1:000", "2.0.0.1:000"]

`groupID`: `string`, 群組代號, 用以紀錄消費訊息的紀錄, 若其他機器設定相同groupID會組成群組, kafka在傳送訊息時僅送給group中的其中一人(監聽相同topic時)

### 傳送訊息

```go
dispatcher.Send(topic, message)
```

`topic`: `string`, 訊息queue的名稱, 監聽時指定相同topic以取得訊息

`message`: `[]byte`, 要傳送的訊息

### 接收訊息
指定topic, 使用callback方法來處理訊息

```go
dispatcher.Subscribe(topic, callback)
```

`callback`: `func (value []byte) error`, 處理訊息

### 可用選項

各API提供可選設定做為非必要參數, 所有可用的選項參見 [options.go](./options.go)

1. Subscriber以多執行續處理訊息
```go
dispatcher.Subscribe(topic, callback, dispatcher.ConsumerSetAsyncNum(5))
```

2. Subscriber只接收自己啟動後的新訊息, 不從未讀過的開始讀
```go
dispatcher.Subscribe(topic, callback, dispatcher.ConsumerOmitOldMsg())
``` 

3. Sender接收Subscriber callback回傳的錯誤
```go
dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))
```

4. Sender確保Subscriber收到訊息是和送出時的順序完全一致, 但會降低效能
```go
dispatcher.Send(topic, msg, dispatcher.ProducerEnsureOrder())
```

