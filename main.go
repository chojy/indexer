package main

import (
	"context"
	"fmt"
	"log"
	_ "os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		//GroupID:  groupID,
		Topic:    topic,
		MinBytes: 100,  //10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	// get kafka reader using environment variables.
	// kafkaURL := os.Getenv("kafkaURL") // kafkaURL: kafka:9092
	// topic := os.Getenv("topic") // topic: topic1
	// groupID := os.Getenv("groupID") // GroupID: logger-group

	kafkaURL := "127.0.0.1:9092"
	topic := "testTopic"
	groupID := "test"

	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		//fmt.Println(m)
		fmt.Printf("message at topic:%v partition:%v offset:%v    %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
