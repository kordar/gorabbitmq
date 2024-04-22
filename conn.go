package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Conn struct {
	conn *amqp.Connection
	dsn  string
}

func (mgr *Conn) Refresh() error {
	// 该连接抽象了套接字连接，并为我们处理协议版本协商和认证等。
	var err error
	mgr.conn, err = amqp.Dial(mgr.dsn)
	return err
}

func (mgr *Conn) GetChannel() (*amqp.Channel, error) {
	return mgr.conn.Channel()
}

func (mgr *Conn) Destroy() {
	_ = mgr.conn.Close()
}

func NewRabbitMQConn(dsn string) *Conn {
	return &Conn{dsn: dsn}
}
