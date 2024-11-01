package rabbitmq

import "github.com/streadway/amqp"

type BaseConsumer struct {
}

func (b *BaseConsumer) QueueName() string {
	return ""
}

func (b *BaseConsumer) RoutingKey() string {
	return ""
}

func (b *BaseConsumer) OnError(err error) {
}

func (b *BaseConsumer) OnReceive(bytes []byte) bool {
	return true
}

func (b *BaseConsumer) QueueDeclareParam() (durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	return false, false, false, false, nil
}

func (b *BaseConsumer) QueueBindParam() (noWait bool, args amqp.Table) {
	return false, nil
}

func (b *BaseConsumer) QosParam() (prefetchCount, prefetchSize int, global bool) {
	return 1, 0, true
}

func (b *BaseConsumer) ConsumeParam() (consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
	return "", false, false, false, false, nil
}
