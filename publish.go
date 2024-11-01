package rabbitmq

import (
	"errors"
	"fmt"
	logger "github.com/kordar/gologger"
	"github.com/streadway/amqp"
)

type ChannelObject struct {
	Name         string
	ExchangeName string
	RoutingKey   string
	QueueName    string
	Mandatory    bool
	Immediate    bool
}

type PublishClient struct {
	channels map[string]*ChannelObject
	conn     *amqp.Connection
	dsn      string
}

func NewPublishClientWithDsn(dsn string) *PublishClient {
	client := PublishClient{
		dsn:      dsn,
		channels: map[string]*ChannelObject{},
	}
	client.Reconnect(true)
	return &client
}

func NewPublishClient(cfg map[string]string) *PublishClient {
	dsn := fmt.Sprintf("%s://%s:%s@%s:%s/", cfg["network"], cfg["username"], cfg["password"], cfg["host"], cfg["port"])
	return NewPublishClientWithDsn(dsn)
}

func (c *PublishClient) Reconnect(flag bool) {
	if conn, err := amqp.Dial(c.dsn); err == nil {
		c.conn = conn
	} else {
		if flag {
			logger.Fatal("connect to the rabbitmq server error")
		}
	}
}

func (c *PublishClient) AddChannelObject(channelObjects ...*ChannelObject) *PublishClient {
	for _, object := range channelObjects {
		c.channels[object.Name] = object
	}
	return c
}

func (c *PublishClient) Channel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

func (c *PublishClient) Publish(name string, body []byte) error {
	return c.PublishByMsg(name, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})
}

func (c *PublishClient) PublishByMsg(name string, msg amqp.Publishing) error {
	if c.channels[name] == nil {
		logger.Warn("please set the channel configuration first and add it to the client")
		return errors.New("channel configuration not set")
	}

	if c.conn.IsClosed() {
		logger.Warn("this client conn is closed, trying to reconnect.")
		_ = c.conn.Close()
		c.Reconnect(false)
		if c.conn.IsClosed() {
			return errors.New("this client reconnect fail")
		}
	}

	object := c.channels[name]
	channel, err := c.conn.Channel()
	if err != nil {
		logger.Warn("get the conn's channel error")
		return err
	}

	return channel.Publish(
		object.ExchangeName, // exchange
		object.RoutingKey,   // routing key
		object.Mandatory,    // mandatory
		object.Immediate,    // immediate
		msg,
	)
}
