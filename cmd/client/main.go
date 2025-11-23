package main

import (
	"fmt"
	"log"
	"time"

	"github.com/x6Nenko/peril/internal/gamelogic"
	"github.com/x6Nenko/peril/internal/pubsub"
	"github.com/x6Nenko/peril/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(publishCh *amqp.Channel, username, message string) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	}

	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
	err := pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, routingKey, gameLog)
	return err
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(publishCh, gs.GetPlayerSnap().Username, message)
			if err != nil {
				fmt.Printf("error: failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(publishCh, gs.GetPlayerSnap().Username, message)
			if err != nil {
				fmt.Printf("error: failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(publishCh, gs.GetPlayerSnap().Username, message)
			if err != nil {
				fmt.Printf("error: failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// Publish war message
			warKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetPlayerSnap().Username)
			rw := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, warKey, rw)
			if err != nil {
				fmt.Printf("error: failed to publish war: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("client welcome error: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create publish channel: %v", err)
	}
	defer publishCh.Close()

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause messages: %v", err)
	}

	// Subscribe to army moves from other players
	armyMovesQueue := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	armyMovesKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		armyMovesQueue,
		armyMovesKey,
		pubsub.Transient,
		handlerMove(gs, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}

	// Subscribe to war messages
	warQueue := "war"
	warKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		warQueue,
		warKey,
		pubsub.Durable,
		handlerWar(gs, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war messages: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// Publish the move to army_moves.username routing key
			moveKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, moveKey, move)
			if err != nil {
				fmt.Printf("error: failed to publish move: %v\n", err)
				continue
			}
			fmt.Printf("Move published successfully to %s\n", moveKey)
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
