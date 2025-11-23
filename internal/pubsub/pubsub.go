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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	// Call DeclareAndBind to ensure the queue exists and is bound to the exchange
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	// Get a channel of deliveries from the queue
	deliveries, err := ch.Consume(
		queueName,
		"",    // consumer name (auto-generated)
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return err
	}

	// Start a goroutine to process messages
	go func() {
		for delivery := range deliveries {
			var msg T
			// Unmarshal the message body into type T
			err := json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				continue
			}

			// Call the handler function with the unmarshaled message
			handler(msg)

			// Acknowledge the message to remove it from the queue
			delivery.Ack(false)
		}
	}()

	return nil
}
