package main

import (
	"fmt"
	"log"

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
		func(ps routing.PlayingState) pubsub.AckAction {
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
	key = routing.GameLogSlug // "game_logs"
	queueName = key

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
		func(move gamelogic.ArmyMove) pubsub.AckAction {
			outcome := gs.HandleMove(move)
			defer fmt.Print("> ")

			switch outcome {
			case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
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

	fmt.Println("Client setup complete! You can now enter commands.")

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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
