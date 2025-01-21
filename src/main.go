package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
)

// Define the structure of the Pub/Sub message payload
type StorageEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	Size   string `json:"size"`
}

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Retrieve configuration from environment variables
	projectID := os.Getenv("GCP_PROJECT_ID")
	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")
	credentialsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	if projectID == "" || subscriptionID == "" || credentialsFile == "" {
		log.Fatalf("Missing required environment variables. Ensure GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION_ID, and GOOGLE_APPLICATION_CREDENTIALS are set.")
	}

	// Set credentials file path
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsFile)

	// Set up the context and Pub/Sub client
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	// Subscribe to messages
	sub := client.Subscription(subscriptionID)

	// 10-second wait before starting the polling
	time.Sleep(time.Second * 10)

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Log the raw message data
		log.Printf("Raw message data: %s", msg.Data)
	
		var event StorageEvent
	
		// Parse the message data (JSON)
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			msg.Nack() // Negative acknowledgment to requeue the message
			return
		}
	
		// Log the event details
		log.Printf("Received event: Bucket=%s File=%s Size=%s", event.Bucket, event.Name, event.Size)
	
		// Handle the video file (e.g., process or move it)
		if err := handleVideoFile(event.Bucket, event.Name); err != nil {
			log.Printf("Error processing video file: %v", err)
			msg.Nack() // Negative acknowledgment in case of processing failure
			return
		}
	
		// Acknowledge the message after successful processing
		msg.Ack()
	
		// 10-second wait after processing each message
		time.Sleep(time.Second * 10)
	})
	
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}
}

// Function to handle video file
func handleVideoFile(bucketName, fileName string) error {
	// Placeholder for processing the video file
	// If an error occurs during processing, return the error
	fmt.Printf("Processing video file: gs://%s/%s\n", bucketName, fileName)

	// Simulate processing success or failure
	// For example, return an error if fileName contains 'error'
	if fileName == "error" {
		return fmt.Errorf("simulated processing error")
	}

	return nil
}
