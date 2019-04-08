# dispatcher

Dispatcher主要功能為透過kafaka任務調度及消息推送, 目前是建構在套件sarama之上

### Producer

 - Send(topic, key, data string)

### Consumer

 - Subscribe(topic string, groupID string, callback ConsumerCallback)