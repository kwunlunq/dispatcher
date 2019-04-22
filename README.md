# dispatcher

Dispatcher主要功能為透過kafaka任務調度及消息推送

參考 [examples](./examples) 使用範例

### Producer

```go
func Send(topic string, key, value []byte, errHandler func(key, value []byte, err error))
```

### Consumer

```go
func Subscribe(topic string, callback func(key, value []byte) error, asyncNum int)
```

### 建置

Windows環境需安裝 [GCC](./build/mingw-w64-install.exe) , Architecure選x86_64

### 設定檔

`app.conf`

```
[dispatcher]
# Kafka集群機器
brokers=10.200.252.180:9092,10.200.252.181:9092,10.200.252.182:9092

# 訂閱代號, 若多台組成群組時, 將多台設定成相同的group_id, 單台時也需設定, 讓kafka紀錄上次的消費位置
group_id=

# TLS相關, 預設不啟用
tls_enable=
verifySsl=
cert_file=
key_file=
ca_file=
```