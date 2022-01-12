package rabbitmq

import "github.com/streadway/amqp"

type RabbitManager interface {
	Refresh() error
	GetChannel() (*amqp.Channel, error)
	Destroy()
}
