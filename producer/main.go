package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "example-topic-123",
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion: func(messages []kafka.Message, err error) {

			if err != nil {
				fmt.Println(err)
				return
			}

			for _, val := range messages {
				fmt.Printf("Message sent, offset %d, key %s, val %s \n", val.Offset, val.Key, val.Value)
			}
		},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("message_name"),
			Value: []byte("message1"),
		},
		kafka.Message{
			Key:   []byte("message_name"),
			Value: []byte("message2"),
		},
		kafka.Message{
			Key:   []byte("message_name"),
			Value: []byte("message3"),
		},
	)

	if err != nil {
		log.Fatal("Failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("Failed to close writer:", err)
	}
}
