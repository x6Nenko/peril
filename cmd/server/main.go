package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/x6Nenko/peril/internal/pubsub"
	"github.com/x6Nenko/peril/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	// Create a new channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}
	defer ch.Close()

	// Publish a pause message
	err = pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)
	if err != nil {
		log.Fatalf("could not publish pause message: %v", err)
	}
	fmt.Println("Pause message published!")

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")
}
