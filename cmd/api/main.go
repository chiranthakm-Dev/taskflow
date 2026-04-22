package main

import (
	"log"
	"net/http"

	"github.com/chiranthakm-Dev/taskflow/internal/api"
	"github.com/chiranthakm-Dev/taskflow/internal/queue"
	"github.com/chiranthakm-Dev/taskflow/internal/store"
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

	router, err := queue.NewRouter(conn, store)
	if err != nil {
		log.Fatal("Failed to create router:", err)
	}

	handler := api.NewHandler(store, router)

	http.HandleFunc("/jobs", handler.HandleJobs)
	http.HandleFunc("/jobs/", handler.HandleJobByID)
	http.HandleFunc("/metrics", handler.HandleMetrics)

	log.Println("Starting API server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}