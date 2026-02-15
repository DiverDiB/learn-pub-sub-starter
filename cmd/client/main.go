package main

import (
	"fmt"
	"log"
	"time"

	"github.com/diverdib/learn-pub-sub-starter/internal/gamelogic"
	"github.com/diverdib/learn-pub-sub-starter/internal/pubsub"
	"github.com/diverdib/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ
	//connectionString := "amqp://guest:guest@127.0.0.1:5672/"
	connectionString := "amqp://guest:guest@host.docker.internal:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Get the username for this client
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	// SUBSCRIBE: Each client gets their own unique, temporary queue for pause events.
	// This allows EVERY player to receive the "Pause" signal simultaneously.
	exch := routing.ExchangePerilDirect
	key := routing.PauseKey
	queueName := key + "." + username

	err = pubsub.SubscribeJSON(
		conn,
		exch,
		queueName,
		key,
		pubsub.Transient,
		func(ch *amqp.Channel, ps routing.PlayingState) pubsub.AckAction {
			gs.HandlePause(ps)
			fmt.Print("> ")
			return pubsub.Ack
		},
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pause messages: %v", err)
	}

	// DECLARE: A shared, durable queue for logging game events.
	// Since the queue name is NOT unique to the client, all clients connect to the
	// SAME queue. This is used for persistent logging of the game's history.
	exch = routing.ExchangePerilTopic
	key = routing.GameLogSlug + ".*" // Listen for "game_logs.<anything>"
	queueName = routing.GameLogSlug

	publishCh, _, err := pubsub.DeclareAndBind(conn, exch, queueName, key, pubsub.Durable)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}

	defer publishCh.Close()

	// SUBSCRIBE: A unique, temporary queue to listen for moves.
	// We use a wildcard (*) so this client receives moves from ANY player.
	// The unique queueName ensures we don't 'steal' move messages from other players.
	exch = routing.ExchangePerilTopic
	key = routing.ArmyMovesPrefix + ".*" // Listen for "army_moves.<anything>"
	queueName = routing.ArmyMovesPrefix + "." + username

	err = pubsub.SubscribeJSON(
		conn,
		exch,
		queueName,
		key,
		pubsub.Transient,
		func(ch *amqp.Channel, move gamelogic.ArmyMove) pubsub.AckAction {
			outcome := gs.HandleMove(move)
			defer fmt.Print("> ")

			switch outcome {
			case gamelogic.MoveOutComeSafe:
				return pubsub.Ack
			case gamelogic.MoveOutcomeMakeWar:
				// Create the War Recognition message
				warMsg := gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				}
				// Publish to the topic exchange
				warKey := routing.WarRecognitionsPrefix + "." + gs.GetPlayerSnap().Username
				err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, warKey, warMsg)
				if err != nil {
					fmt.Printf("Failed to publish war recognition: %v", err)
					return pubsub.NackRequeue
				}
				return pubsub.Ack
			case gamelogic.MoveOutcomeSamePlayer:
				return pubsub.NackDiscard
			default:
				return pubsub.NackDiscard
			}
		},
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to move messages: %v", err)
	}

	// All clients share the war queue
	warQueueName := routing.WarRecognitionsPrefix
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		warQueueName,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		func(ch *amqp.Channel, rw gamelogic.RecognitionOfWar) pubsub.AckAction {
			defer fmt.Print("> ")

			// Process the war through the game engine
			outcome, winner, loser := gs.HandleWar(rw)

			switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue
			case gamelogic.WarOutcomeNoUnits:
				return pubsub.NackDiscard
			case gamelogic.WarOutcomeOpponentWon,
				gamelogic.WarOutcomeYouWon,
				gamelogic.WarOutcomeDraw:
				// Determine the log message based on the outcome
				var message string
				if outcome == gamelogic.WarOutcomeDraw {
					message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
				} else {
					message = fmt.Sprintf("%s won a war against %s", winner, loser)
				}
				// Format the routine key as "game_logs.<attacker_username>"
				logKey := routing.GameLogSlug + "." + rw.Attacker.Username
				// Publish the full GameLog structure to the topic exchange
				fmt.Printf("Attempting to publish log for %s...\n", rw.Attacker.Username)
				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					logKey,
					routing.GameLog{
						Username:    rw.Attacker.Username,
						CurrentTime: time.Now(),
						Message:     message,
					},
				)
				if err != nil {
					fmt.Printf("Error publishing log:  %v\n", err)
					return pubsub.NackRequeue
				}
				fmt.Printf("War resolved and logged: %s\n", message)
				return pubsub.Ack
			default:
				fmt.Printf("Error: Unknown war outcome: %v\n", outcome)
				return pubsub.NackDiscard
			}
		},
	)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
				continue
			}
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			}
			key := routing.ArmyMovesPrefix + "." + username
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				key,
				move,
			)
			if err != nil {
				fmt.Printf("Error publishing move: %v\n", err)
				continue
			}

		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			continue
		}
	}
}

// 1. Update the return type signature to include *amqp.Channel
func handlerPause(gs *gamelogic.GameState) func(*amqp.Channel, routing.PlayingState) pubsub.AckAction {
	// 2. Update the inner function to accept the channel (even if we don't use it)
	return func(ch *amqp.Channel, ps routing.PlayingState) pubsub.AckAction {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}
