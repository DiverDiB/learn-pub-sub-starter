package main

import (
	// Local packages
	"github.com/diverdib/learn-pub-sub-starter/internal/pubsub"
	"github.com/diverdib/learn-pub-sub-starter/internal/routing"

	// Standard library packages
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	//connectionString := "amqp://guest:guest@localhost:5672/"
	connectionString := "amqp://guest:guest@host.docker.internal:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ successfully!")

	rmqChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	//defer rmqChannel.Close()

	fmt.Println("Channel opened successfully!")
	// Create the state we want to send to the client
	state := routing.PlayingState{
		IsPaused: true,
	}
	fmt.Println("DEBUG exchange:", routing.ExchangePerilDirect)
	fmt.Println("DEBUG pause key:", routing.PauseKey)
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
	// ... after your PublishJSON call ...

	fmt.Println("Message published. Keeping connection open for 30 seconds...")
	fmt.Println("Check the Connections tab now!")

	// Block here so the connection stays visible in the UI
	time.Sleep(30 * time.Second)
	//fmt.Println("Pause message published successfully!")
	// Wait for signal to exit (e.g., Ctrl+C) to exit gracefully
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	// Block until a signal is received
	<-signalChan
	// Perform any necessary cleanup here before exiting
	fmt.Println("\nReceived interrupt signal, shutting down...")

}
