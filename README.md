# gorabbitmq
`rabbitmq`使用统一`publish`发布消息、`consumer`消费消息，实现`Receiver`接口实现`consumer`统一管理调用。

## 消费者定义

- 定义消费者处理器
```go
type Demo3Consumer struct {
    *rabbitmq.BaseConsumer
}

func (b *Demo3Consumer) QueueName() string {
    return "demo_topic3"
}

func (b *Demo3Consumer) RoutingKey() string {
    return "#"
}

func (b *Demo3Consumer) OnReceive(bytes []byte) bool {
    logger.Infof("%s========================%v", b.QueueName(), string(bytes))
    return true // 返回true将自动ack
}
```

- 启动消费端

```go
// 定义交换机
client := rabbitmq.NewDefaultRabbitMQ("demo", amqp.ExchangeDirect)
// 创建amqp句柄
conn := rabbitmq.NewDefaultConn("amqp://admin:admin@192.168.30.128:5672/%2f")
// 将消费处理器注入消费端client
client.RegisterReceiver(&DemoConsumer{})
// 启动消费端
client.Start(conn)
```

## 生产者定义

- 生产对象配置

```go
rabbitmq.ChannelObject{
    Name:         "test",
    ExchangeName: "demo",
    RoutingKey:    "demo",
    QueueName:    "demo",
}
```

- 启动生产端

```go
// 创建生产端
publishClient := rabbitmq.NewPublishClientWithDsn("amqp://admin:admin@192.168.30.128:5672/%2f")
// 生产端添加配置对象
publishClient.AddChannelObject(&rabbitmq.ChannelObject{
    Name:         "test",
    ExchangeName: "demo",
    RoutingKey:    "demo",
    QueueName:    "demo",
})
// 投递消息
publishClient.Publish("test", []byte("AAAAAAAAAAAA"))
// 也可直接获取channel进行投递
publishClient.Channel()


```