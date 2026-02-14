package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/diverdib/learn-pub-sub-starter/internal/routing"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

// AckAction is our custom "acktype"
type AckAction int

const (
	// Ack: Processed successfully
	Ack AckAction = iota
	// NackRequeue: Not processed successfully, but should be retried
	NackRequeue
	// NackDiscard: Not processed successfully, and should be deleted
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// Marshal the value to JSON
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// PublisthWithContext to the exchange with the routing key
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})

}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckAction,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			var val T
			if err := json.Unmarshal(d.Body, &val); err != nil {
				continue
			}
			action := handler(val)

			// Respond to RabbitMQ baed on the handler's AckAction
			switch action {
			case Ack:
				d.Ack(false)
				fmt.Println("Message Acked")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("Message Nacked and Requeued")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("Message Nacked and Discarded")
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := queueType == Transient
	args := amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDLX,
	}
	queue, err := ch.QueueDeclare(
		queueName,
		durable,    // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		false,      // no-wait
		args,       // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}
