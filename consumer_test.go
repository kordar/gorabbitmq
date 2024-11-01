package rabbitmq_test

import (
	logger "github.com/kordar/gologger"
	rabbitmq "github.com/kordar/gorabbitmq"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

type DemoConsumer struct {
	*rabbitmq.BaseConsumer
}

func (b *DemoConsumer) QueueName() string {
	return "demo_topic1"
}

func (b *DemoConsumer) RoutingKey() string {
	return "com.demo.*"
}

func (b *DemoConsumer) OnReceive(bytes []byte) bool {
	logger.Infof("%s========================%v", b.QueueName(), string(bytes))
	return true
}

// =================================
type Demo2Consumer struct {
	*rabbitmq.BaseConsumer
}

func (b *Demo2Consumer) QueueName() string {
	return "demo_topic2"
}

func (b *Demo2Consumer) RoutingKey() string {
	return "com.demo.topic2"
}

func (b *Demo2Consumer) OnReceive(bytes []byte) bool {
	logger.Infof("%s========================%v", b.QueueName(), string(bytes))
	return true
}

func (b *Demo2Consumer) QueueDeclareParam() (durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	return true, false, false, false, map[string]interface{}{"CCC": "hello"}
}

// =================================
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
	return true
}

func TestNewConsumer(t *testing.T) {
	mq := rabbitmq.NewDefaultRabbitMQ("demo", amqp.ExchangeDirect)
	conn := rabbitmq.NewDefaultConn("amqp://admin:admin@192.168.30.128:5672/%2f")
	mq.RegisterReceiver(&DemoConsumer{})
	mq.Start(conn)
	time.Sleep(100000)
}

func TestNewPublishClient(t *testing.T) {
	publishClient := rabbitmq.NewPublishClientWithDsn("amqp://admin:admin@192.168.30.128:5672/%2f")
	publishClient.AddChannelObject(&rabbitmq.ChannelObject{
		Name:         "test",
		ExchangeName: "demo",
		RoutingKey:   "demo",
		QueueName:    "demo",
	})
	publishClient.Publish("test", []byte("AAAAAAAAAAAA"))
}

func TestTopicExchange(t *testing.T) {
	mq := rabbitmq.NewRabbitMQ("demo_topic", amqp.ExchangeTopic, true, false, false, false, nil)
	conn := rabbitmq.NewDefaultConn("amqp://admin:yunpengai.306@192.168.30.128:5672/%2f")
	mq.RegisterReceiver(&DemoConsumer{})
	mq.RegisterReceiver(&Demo2Consumer{})
	mq.RegisterReceiver(&Demo3Consumer{})
	mq.Start(conn)
	time.Sleep(100000)
}

func TestNewPublishTopicClient(t *testing.T) {
	publishClient := rabbitmq.NewPublishClientWithDsn("amqp://admin:yunpengai.306@192.168.30.128:5672/%2f")
	publishClient.AddChannelObject(&rabbitmq.ChannelObject{
		Name:         "test",
		ExchangeName: "demo_topic",
		RoutingKey:   "com.demo.topic2",
	})
	publishClient.Publish("test", []byte("CCCC"))
}
