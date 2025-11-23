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

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	// Create a new channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// Determine queue parameters based on queueType
	durable := queueType == Durable
	autoDelete := queueType == Transient
	exclusive := queueType == Transient

	// Declare the queue
	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// Bind the queue to the exchange
	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// Marshal the value to JSON bytes
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// Publish the message to the exchange with the routing key
	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
