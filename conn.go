package rabbitmq

import "github.com/streadway/amqp"

type Conn interface {
	Refresh() error
	GetChannel() (*amqp.Channel, error)
	Destroy()
}

type DefaultConn struct {
	conn *amqp.Connection
	dsn  string
}

func (c *DefaultConn) Refresh() error {
	// 该连接抽象了套接字连接，并为我们处理协议版本协商和认证等。
	var err error
	c.conn, err = amqp.Dial(c.dsn)
	return err
}

func (c *DefaultConn) GetChannel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

func (c *DefaultConn) Destroy() {
	_ = c.conn.Close()
}

func NewRabbitMQConn(dsn string) Conn {
	return &DefaultConn{dsn: dsn}
}
