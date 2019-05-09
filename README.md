# dispatcher

Dispatcher主要功能為透過kafaka任務調度及消息推送

參考 [producer](./examples/producer/main.go), [consumer](./examples/consumer/main.go) 使用範例

## Quick Start

### Initialize
在使用其他方法前, 須先完成初始化

`brokers`: `[]string`, kafka機器ip清單, ex. ["1.0.0.1:000", "2.0.0.1:000"]

`groupID`: `string`, 群組代號, 用以紀錄消費訊息的紀錄, 若其他機器設定相同groupID會組成群組, kafka在傳送訊息時僅送給group中的其中一人(監聽相同topic時)

```go
dispatcher.Init(brokers, groupID)
```

### Send
傳送訊息

`topic`: `string`, 訊息queue的名稱, 監聽時指定相同topic以取得訊息

`message`: `[]byte`, 要傳送的訊息

```go
dispatcher.Send(topic, message)
```

### Receive
接收訊息, 指定topic, 使用callback方法來處理訊息

```go
dispatcher.Subscribe(topic, func (value []byte) error {
	// Process message ...
	return nil
})
```


## 建置

Windows環境需安裝 [GCC](./build/mingw-w64-install.exe) , Architecure選x86_64
