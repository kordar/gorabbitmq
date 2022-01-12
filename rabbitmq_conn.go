package rabbitmq

import (
	"github.com/streadway/amqp"
)

type RabbitMQConn struct {
	conn *amqp.Connection
	dsn  string
}

func (mgr *RabbitMQConn) Refresh() error {
	// 该连接抽象了套接字连接，并为我们处理协议版本协商和认证等。
	var err error
	mgr.conn, err = amqp.Dial(mgr.dsn)
	return err
}

func (mgr *RabbitMQConn) GetChannel() (*amqp.Channel, error) {
	return mgr.conn.Channel()
}

func (mgr *RabbitMQConn) Destroy() {
	_ = mgr.conn.Close()
}

func NewRabbitMQConn(dsn string) *RabbitMQConn {
	return &RabbitMQConn{dsn: dsn}
}
