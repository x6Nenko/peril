package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
