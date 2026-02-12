package main

import (
	// Local packages
	"github.com/diverdib/learn-pub-sub-starter/internal/gamelogic"
	"github.com/diverdib/learn-pub-sub-starter/internal/pubsub"
	"github.com/diverdib/learn-pub-sub-starter/internal/routing"

	// Standard library packages
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//connectionString := "amqp://guest:guest@127.0.0.1:5672/"
	connectionString := "amqp://guest:guest@host.docker.internal:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Server connected to RabbitMQ successfully!")

	rmqChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer rmqChannel.Close()

	fmt.Println("Channel opened successfully!")

	// Declare the exchange (if it doesn't already exist)
	err = rmqChannel.ExchangeDeclare(
		routing.ExchangePerilDirect, // name
		"direct",                    // type
		true,                        // durable
		false,                       // auto-deleted
		false,                       // internal
		false,                       // no-wait
		nil,                         // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}
	fmt.Printf("Exchange %s declared successfully!\n", routing.ExchangePerilDirect)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "pause":
			fmt.Println("Pausing the game...")
			// Create the state we want to send to the client
			state := routing.PlayingState{
				IsPaused: true,
			}
			err = pubsub.PublishJSON(
				rmqChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				state,
			)

			if err != nil {
				log.Fatalf("Failed to publish game state: %v", err)
			}
			fmt.Printf("Publishing to %s with key %s: %+v\n", routing.ExchangePerilDirect, routing.PauseKey, state)
		case "resume":
			fmt.Println("Resuming the game...")
			state := routing.PlayingState{
				IsPaused: false,
			}
			err = pubsub.PublishJSON(
				rmqChannel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				state,
			)

			if err != nil {
				log.Fatalf("Failed to publish game state: %v", err)
			}
			fmt.Printf("Publishing to %s with key %s: %+v\n", routing.ExchangePerilDirect, routing.PauseKey, state)
		case "help":
			gamelogic.PrintServerHelp()
		case "quit":
			fmt.Println("Quitting the server...")
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			gamelogic.PrintServerHelp()
		}
	}
}
