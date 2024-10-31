package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type ExchangeType string

var (
	Direct ExchangeType = "direct"
	Topic  ExchangeType = "topic"
	Fanout ExchangeType = "fanout"
	Header ExchangeType = "header"
)

// Receiver 观察者模式需要的接口
// 观察者用于接收指定的queue到来的数据
type Receiver interface {
	QueueName() string     // 获取接收者需要监听的队列
	RouterKey() string     // 这个队列绑定的路由
	OnError(error)         // 处理遇到的错误，当RabbitMQ对象发生了错误，他需要告诉接收者处理错误
	OnReceive([]byte) bool // 处理收到的消息, 这里需要告知RabbitMQ对象消息是否处理成功
}

// RabbitMQ 用于管理和维护rabbitmq的对象
type RabbitMQ struct {
	wg sync.WaitGroup

	queueDurable    bool
	exchangeDurable bool
	channel         *amqp.Channel
	exchangeName    string       // exchange的名称
	exchangeType    ExchangeType // exchange的类型
	receivers       []Receiver
	// 如果 autoDelete 设置为 true，当最后一个消费者取消订阅后，队列（或交换器）会被自动删除。
	// 适用于不需要长期保存的临时队列，比如只为某个短期任务或事件服务的队列。
	autoDelete bool
	// 如果 exclusive 设置为 true，队列将只对声明它的连接可见，即只有该连接可以使用此队列。
	// 一旦该连接关闭，独占队列会被自动删除。
	// 常用于私有或一次性使用的队列场景，例如在远程过程调用（RPC）中，一个客户端需要访问的专用队列。
	exclusive bool
	// 如果 noWait 设置为 true，客户端在声明队列、交换器或绑定后，不等待代理（broker）的响应。
	// 这样可以减少延迟，因为客户端无需等待确认消息。
	// 但如果出现问题（如队列已存在但属性不同），不会立即得到反馈，可能会增加调试难度。
	noWait bool
	args   amqp.Table
}

// RegisterReceiver 注册一个用于接收指定队列指定路由的数据接收者
func (mq *RabbitMQ) RegisterReceiver(receiver Receiver) {
	mq.receivers = append(mq.receivers, receiver)
}

// Listen 监听指定路由发来的消息
// 这里需要针对每一个接收者启动一个goroutine来执行listen
// 该方法负责从每一个接收者监听的队列中获取数据，并负责重试
func (mq *RabbitMQ) listen(receiver Receiver) {
	defer mq.wg.Done()

	// 这里获取每个接收者需要监听的队列和路由
	queueName := receiver.QueueName()
	routerKey := receiver.RouterKey()

	// 申明Queue
	_, err := mq.channel.QueueDeclare(
		queueName,       // name
		mq.queueDurable, // durable
		false,           // delete when unused
		false,           // exclusive(排他性队列)
		false,           // no-wait
		nil,             // arguments
	)
	if nil != err {
		// 当队列初始化失败的时候，需要告诉这个接收者相应的错误
		receiver.OnError(fmt.Errorf("初始化队列 %s 失败: %s", queueName, err.Error()))
	}

	// 将Queue绑定到Exchange上去
	err = mq.channel.QueueBind(
		queueName,       // queue name
		routerKey,       // routing key
		mq.exchangeName, // exchange
		false,           // no-wait
		nil,
	)

	if nil != err {
		receiver.OnError(fmt.Errorf("绑定队列 [%s - %s] 到交换机失败: %s", queueName, routerKey, err.Error()))
	}

	// 获取消费通道
	mq.channel.Qos(1, 0, true) // 确保rabbitmq会一个一个发消息
	msgs, err := mq.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if nil != err {
		receiver.OnError(fmt.Errorf("获取队列 %s 的消费通道失败: %s", queueName, err.Error()))
	}

	// 使用callback消费数据
	for msg := range msgs {
		// 当接收者消息处理失败的时候，
		// 比如网络问题导致的数据库连接失败，redis连接失败等等这种
		// 通过重试可以成功的操作，那么这个时候是需要重试的
		// 直到数据处理成功后再返回，然后才会回复rabbitmq ack
		for !receiver.OnReceive(msg.Body) {
			log.Println("receiver 数据处理失败，将要重试")
			time.Sleep(1 * time.Second)
		}

		// 确认收到本条消息, multiple必须为false
		msg.Ack(false)
	}
}

// prepareExchange 准备rabbitmq的Exchange
func (mq *RabbitMQ) prepareExchange() error {
	// 申明Exchange
	err := mq.channel.ExchangeDeclare(
		mq.exchangeName,    // exchange
		mq.exchangeType,    // type
		mq.exchangeDurable, // durable
		false,              // autoDelete
		false,              // internal
		false,              // noWait
		nil,                // args
	)

	if nil != err {
		return err
	}

	return nil
}

// run 开始获取连接并初始化相关操作
func (mq *RabbitMQ) run(manager RabbitManager) {
	if err := manager.Refresh(); err != nil {
		log.Println("rabbit刷新连接失败，将要重连...")
		return
	}

	// 获取新的channel对象
	var channelError error
	mq.channel, channelError = manager.GetChannel()
	if channelError != nil {
		log.Println("rabbit获取channel异常")
		return
	}

	// 初始化Exchange
	if channelError = mq.prepareExchange(); channelError != nil {
		log.Println("初始化Exchange失败")
		return
	}

	for _, receiver := range mq.receivers {
		mq.wg.Add(1)
		go mq.listen(receiver) // 每个接收者单独启动一个goroutine用来初始化queue并接收消息
	}

	mq.wg.Wait()

	log.Println("所有处理queue的任务都意外退出了")

	// 理论上mq.run()在程序的执行过程中是不会结束的
	// 一旦结束就说明所有的接收者都退出了，那么意味着程序与rabbitmq的连接断开
	// 那么则需要重新连接，这里尝试销毁当前连接
	manager.Destroy()
}

// Start 启动Rabbitmq的客户端
func (mq *RabbitMQ) Start(manager RabbitManager) {
	for {
		mq.run(manager)
		// 一旦连接断开，那么需要隔一段时间去重连
		// 这里最好有一个时间间隔
		time.Sleep(3 * time.Second)
	}
}

func (mq *RabbitMQ) StartD(manager RabbitManager, dur time.Duration) {
	for {
		mq.run(manager)
		// 一旦连接断开，那么需要隔一段时间去重连
		// 这里最好有一个时间间隔
		time.Sleep(dur)
	}
}

// NewRabbitMQ New 创建一个新的操作RabbitMQ的对象
func NewRabbitMQ(exchangeName string, exchangeType string) *RabbitMQ {
	// 这里可以根据自己的需要去定义
	return &RabbitMQ{
		exchangeName: exchangeName,
		exchangeType: exchangeType,
	}
}

func NewRabbitMQWithDurable(exchangeName string, exchangeType string, exchangeDurable bool, queueDurable bool) *RabbitMQ {
	// 这里可以根据自己的需要去定义
	return &RabbitMQ{
		exchangeName:    exchangeName,
		exchangeType:    exchangeType,
		exchangeDurable: exchangeDurable,
		queueDurable:    queueDurable,
	}
}
