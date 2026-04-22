package main

import (
	"log"

	"github.com/chiranthakm-Dev/taskflow/handlers"
	"github.com/chiranthakm-Dev/taskflow/internal/store"
	"github.com/chiranthakm-Dev/taskflow/internal/worker"
	"github.com/streadway/amqp"
)

func main() {
	// Initialize store
	store, err := store.NewStore("postgres://taskflow:taskflow@localhost/taskflow?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// Initialize RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	w, err := worker.NewWorker(conn, store)
	if err != nil {
		log.Fatal("Failed to create worker:", err)
	}

	// Register handlers
	w.RegisterHandler("send_email", &handlers.SendEmailHandler{})

	log.Println("Starting worker")
	w.Start()

	// Keep running
	select {}
}