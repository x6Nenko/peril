package main

import (
	"fmt"
	"log"

	"github.com/x6Nenko/peril/internal/gamelogic"
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

	// Subscribe to game_logs queue
	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".#",
		pubsub.Durable,
		func(gamelog routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			err := gamelogic.WriteLog(gamelog)
			if err != nil {
				log.Printf("could not write log: %v", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		},
	)
	if err != nil {
		log.Fatalf("could not subscribe to game_logs queue: %v", err)
	}

	// Print server help
	gamelogic.PrintServerHelp()

	// Start infinite loop for REPL
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Printf("could not publish pause message: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Printf("could not publish resume message: %v", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
