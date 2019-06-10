# Changelog

### Version 2.1.0 (2019-06-10)

Improvements:
- Add consumer option: setGroupID

### Version 2.0.0 (2019-05-09)

Features:
- Introduce functional options to public APIs.
- Init() method to get necessary setting value instead of reading config file directly.
- Utilize `go.uber.org/zap` as project's logger.

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