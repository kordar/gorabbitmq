package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type ChannelObject struct {
	Channel      *amqp.Channel
	Name         string
	ExchangeName string
	RouterKey    string
	QueueName    string
	Mandatory    bool
	Immediate    bool
}

type PublishClient struct {
	onceofchannel map[string]*sync.Once
	channels      map[string]*ChannelObject
	conn          *amqp.Connection
}

func NewPublishClient(cfg map[string]string) *PublishClient {
	dsn := fmt.Sprintf("%s://%s:%s@%s:%s/", cfg["network"], cfg["username"], cfg["password"], cfg["host"], cfg["port"])
	conn, err := amqp.Dial(dsn)
	failOnError(err, "Failed to connect to RabbitMQ")
	return &PublishClient{
		conn:          conn,
		onceofchannel: map[string]*sync.Once{},
		channels:      map[string]*ChannelObject{},
	}
}

func (c *PublishClient) CreateChannel(channelObject *ChannelObject, e func(channel *amqp.Channel), q func(channel *amqp.Channel), b func(channel *amqp.Channel)) *PublishClient {
	name := channelObject.Name
	if c.onceofchannel[name] == nil {
		c.onceofchannel[name] = &sync.Once{}
	}
	c.onceofchannel[name].Do(func() {
		var errChannel error
		c.channels[name] = channelObject
		c.channels[name].Channel, errChannel = c.conn.Channel()
		failOnError(errChannel, "Failed to open a channel")
		e(c.channels[name].Channel)
		q(c.channels[name].Channel)
		b(c.channels[name].Channel)
	})

	return c
}

func (c *PublishClient) Publish(name string, body []byte) error {
	if c.channels[name] == nil {
		return errors.New("channel not found")
	}
	object := c.channels[name]
	return object.Channel.Publish(
		object.ExchangeName, // exchange
		object.RouterKey,    // routing key
		object.Mandatory,    // mandatory
		object.Immediate,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
}
