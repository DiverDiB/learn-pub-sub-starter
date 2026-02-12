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

	//connectionString := "amqp://guest:guest@127.0.0.1:5672/"
	connectionString := "amqp://guest:guest@host.docker.internal:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Client connected to RabbitMQ successfully!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	exch := routing.ExchangePerilDirect
	key := routing.PauseKey
	queueName := key + "." + username

	ch, queue, err := pubsub.DeclareAndBind(conn, exch, queueName, key, pubsub.Transient)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}
	defer ch.Close()

	fmt.Printf("Queue %s declared and bound successfully!\n", queue.Name)

	// Initialize the game state for the client
	gs := gamelogic.NewGameState(username)

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
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
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
