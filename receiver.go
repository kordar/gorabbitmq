package rabbitmq

import "github.com/streadway/amqp"

type BaseReceiver struct {
}

func (b *BaseReceiver) QueueName() string {
	return ""
}

func (b *BaseReceiver) RouteKey() string {
	return ""
}

func (b *BaseReceiver) OnError(err error) {
}

func (b *BaseReceiver) OnReceive(bytes []byte) bool {
	return true
}

func (b *BaseReceiver) QueueDeclareParam() (name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	return b.QueueName(), false, false, false, false, nil
}

func (b *BaseReceiver) QueueBindParam() (name, key string, noWait bool, args amqp.Table) {
	return b.QueueName(), b.RouteKey(), false, nil
}

func (b *BaseReceiver) QosParam() (prefetchCount, prefetchSize int, global bool) {
	return 1, 0, true
}

func (b *BaseReceiver) ConsumeParam() (queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
	return b.QueueName(), "", false, false, false, false, nil
}
