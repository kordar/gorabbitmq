package rabbitmq

import (
	"fmt"
	logger "github.com/kordar/gologger"
	"github.com/streadway/amqp"
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
	OnError(error)         // 处理遇到的错误，当RabbitMQ对象发生了错误，他需要告诉接收者处理错误
	OnReceive([]byte) bool // 处理收到的消息, 这里需要告知RabbitMQ对象消息是否处理成功
	QueueDeclareParam() (name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table)
	QueueBindParam() (name, key string, noWait bool, args amqp.Table)
	QosParam() (prefetchCount, prefetchSize int, global bool)
	ConsumeParam() (queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table)
}

// RabbitMQ 用于管理和维护rabbitmq的对象
type RabbitMQ struct {
	wg        sync.WaitGroup
	channel   *amqp.Channel
	receivers []Receiver

	exchangeName       string       // exchange的名称
	exchangeType       ExchangeType // exchange的类型
	exchangeDurable    bool
	exchangeAutoDelete bool
	exchangeInternal   bool
	exchangeNoWait     bool
	exchangeArgs       amqp.Table

	// 如果 autoDelete 设置为 true，当最后一个消费者取消订阅后，队列（或交换器）会被自动删除。
	// 适用于不需要长期保存的临时队列，比如只为某个短期任务或事件服务的队列。
	//autoDelete bool
	// 如果 exclusive 设置为 true，队列将只对声明它的连接可见，即只有该连接可以使用此队列。
	// 一旦该连接关闭，独占队列会被自动删除。
	// 常用于私有或一次性使用的队列场景，例如在远程过程调用（RPC）中，一个客户端需要访问的专用队列。
	//exclusive bool
	// 如果 noWait 设置为 true，客户端在声明队列、交换器或绑定后，不等待代理（broker）的响应。
	// 这样可以减少延迟，因为客户端无需等待确认消息。
	// 但如果出现问题（如队列已存在但属性不同），不会立即得到反馈，可能会增加调试难度。
	//noWait bool
	//args   amqp.Table
}

// RegisterReceiver 注册一个用于接收指定队列指定路由的数据接收者
func (mq *RabbitMQ) RegisterReceiver(receiver Receiver) {
	mq.receivers = append(mq.receivers, receiver)
}

// prepareExchange 准备rabbitmq的Exchange
func (mq *RabbitMQ) prepareExchange() error {
	// 申明Exchange
	return mq.channel.ExchangeDeclare(
		mq.exchangeName,         // exchange
		string(mq.exchangeType), // type
		mq.exchangeDurable,      // durable
		mq.exchangeAutoDelete,   // autoDelete
		mq.exchangeInternal,     // internal
		mq.exchangeNoWait,       // noWait
		mq.exchangeArgs,         // args
	)
}

// run 开始获取连接并初始化相关操作
func (mq *RabbitMQ) run(conn Conn) {
	if err := conn.Refresh(); err != nil {
		logger.Warn("rabbitmq connection failed, will reconnect...")
		return
	}

	// 获取新的channel对象
	var channelError error
	mq.channel, channelError = conn.GetChannel()
	if channelError != nil {
		logger.Warnf("rabbitmq get channel exception, %v", channelError)
		return
	}

	// 初始化Exchange
	if channelError = mq.prepareExchange(); channelError != nil {
		logger.Warn("rabbitmq init exchange failed, will reconnect...")
		return
	}

	for _, receiver := range mq.receivers {
		mq.wg.Add(1)
		go mq.listen(receiver) // 每个接收者单独启动一个goroutine用来初始化queue并接收消息
	}

	mq.wg.Wait()

	logger.Warn("all tasks processing the queue have exited unexpectedly")

	// 理论上mq.run()在程序的执行过程中是不会结束的
	// 一旦结束就说明所有的接收者都退出了，那么意味着程序与rabbitmq的连接断开
	// 那么则需要重新连接，这里尝试销毁当前连接
	conn.Destroy()
}

// Start 启动Rabbitmq的客户端
func (mq *RabbitMQ) Start(conn Conn) {
	mq.StartD(conn, 3*time.Second)
}

func (mq *RabbitMQ) StartD(conn Conn, dur time.Duration) {
	for {
		mq.run(conn)
		// 一旦连接断开，那么需要隔一段时间去重连
		// 这里最好有一个时间间隔
		time.Sleep(dur)
	}
}

// NewDefaultRabbitMQ New 创建一个新的操作RabbitMQ的对象
func NewDefaultRabbitMQ(exchangeName string, exchangeType ExchangeType) *RabbitMQ {
	return NewRabbitMQ(exchangeName, exchangeType, false, false, false, false, nil)
}

func NewRabbitMQ(exchangeName string, exchangeType ExchangeType, exchangeDurable bool, exchangeAutoDelete bool, exchangeInternal bool, exchangeNoWait bool, exchangeArgs amqp.Table) *RabbitMQ {
	return &RabbitMQ{
		exchangeName:       exchangeName,
		exchangeType:       exchangeType,
		exchangeDurable:    exchangeDurable,
		exchangeInternal:   exchangeInternal,
		exchangeAutoDelete: exchangeAutoDelete,
		exchangeNoWait:     exchangeNoWait,
		exchangeArgs:       exchangeArgs,
	}
}

// Listen 监听指定路由发来的消息
// 这里需要针对每一个接收者启动一个goroutine来执行listen
// 该方法负责从每一个接收者监听的队列中获取数据，并负责重试
func (mq *RabbitMQ) listen(receiver Receiver) {
	defer mq.wg.Done()

	// 申明Queue
	qname, qdurable, qautoDelete, qexclusive, qnowait, qargs := receiver.QueueDeclareParam()
	_, err := mq.channel.QueueDeclare(qname, qdurable, qautoDelete, qexclusive, qnowait, qargs)

	if nil != err {
		// 当队列初始化失败的时候，需要告诉这个接收者相应的错误
		receiver.OnError(fmt.Errorf("initialize the queue [%s] failed, %s", qname, err.Error()))
	}

	bname, bkey, bnoWait, bargs := receiver.QueueBindParam()
	// 将Queue绑定到Exchange上去
	err = mq.channel.QueueBind(bname, bkey, mq.exchangeName, bnoWait, bargs)

	if nil != err {
		receiver.OnError(fmt.Errorf("binding queue \"%s\" use \"%s\" to the exchange error, %s", bname, bkey, err.Error()))
	}

	// 获取消费通道
	qoscount, qossize, qosglobal := receiver.QosParam()
	err = mq.channel.Qos(qoscount, qossize, qosglobal) // 确保rabbitmq会一个一个发消息
	if err != nil {
		receiver.OnError(fmt.Errorf("set Qos error, %s", err.Error()))
	}

	msgs, err := mq.channel.Consume(receiver.ConsumeParam())

	if nil != err {
		receiver.OnError(fmt.Errorf("get the queue \"%s\" for the comsume channel error, %s", qname, err.Error()))
	}

	// 使用callback消费数据
	for msg := range msgs {
		// 当接收者消息处理失败的时候，
		// 比如网络问题导致的数据库连接失败，redis连接失败等等这种
		// 通过重试可以成功的操作，那么这个时候是需要重试的
		// 直到数据处理成功后再返回，然后才会回复rabbitmq ack
		for !receiver.OnReceive(msg.Body) {
			logger.Error("receiver handle the msg failed, will be retry.")
			time.Sleep(1 * time.Second)
		}

		// 确认收到本条消息, multiple必须为false
		_ = msg.Ack(false)
	}
}
