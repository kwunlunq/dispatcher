# Changelog

#### Version 1.2.0 (2019-04-17)

Improvment
- Subscriber now accept nil callback param
- Establish new client for each subscriber, so multiple Producer/Subscriber is possible
- 解除部分panic, 讓sarama接手自動重新連接. Ex.新consumer加入group導致原有consumer收到錯誤

### Version 1.1.1 (2019-04-17)

Minor Update:
- Update go.mod: yaitoo to latest

#### Version 1.1.0 (2019-04-17)

New Features:
- Create topic automatically before sending/subscribing

Bug Fixes:
- Use shared client instead of creating one everytime, trying to fix `Failed to start Sarama producer: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`

#### Version 1.0.0 (2019-04-15)

New Features:
- Add async ablility to Consumer

#### Version 0.1.0 (2019-04-08)

First tagged version.

New Features:
- Consumer & Producer