# dispatcher

Dispatcher主要功能為透過kafaka任務調度及消息推送

參考 [producer](./examples/producer), [consumer](./examples/consumer) 使用範例

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

### 必填 ###
# Kafka集群機器
brokers=10.200.252.180:9092,10.200.252.181:9092,10.200.252.182:9092
# 訂閱訊息的識別代號, 若多台設定相同時, 組成群組; 未填時使用uuid, 會每次重收全部訊息
group_id=

### 選填 ###
# Partition數量, 影響consumer多工, 預設10
topic_partition_num=
# 訊息備份數量(包含leader), 預設2
topic_replication_num=
# 預設20M
msg_max_bytes=
# TLS相關, 預設不啟用
tls_enable=
verifySsl=
cert_file=
key_file=
ca_file=
```