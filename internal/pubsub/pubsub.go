package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
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
	handler func(T),
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
			handler(val)
			// Acknowledge the message with delivery.Ack(false)
			d.Ack(false)
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
	queue, err := ch.QueueDeclare(
		queueName,
		durable,    // durable
		autoDelete, // delete when unused
		exclusive,  // exclusive
		false,      // no-wait
		nil,        // arguments
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
