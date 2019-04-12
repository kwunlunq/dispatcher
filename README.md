# dispatcher

Dispatcher主要功能為透過kafaka任務調度及消息推送

參考 [examples](./examples) 使用範例

### Producer

```go
func Send(topic, key, val []byte)
```

### Consumer

```go
func Subscribe(topic string, groupID string, callback ConsumerCallback)
```

`app.conf`

```
[dispatcher]
ip_list=10.200.252.180:9092,10.200.252.181:9092,10.200.252.182:9092
tls_enable=
verifySsl=
cert_file=
key_file=
ca_file=
```