// =============================================================================
//
// Produce messages to Confluent Cloud
// Using Confluent Golang Client for Apache Kafka
//
// =============================================================================

package main

/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"./ccloud"
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"time"
	"github.com/segmentio/ksuid"
	"strconv"
)

// RecordValue represents the struct of the value in a Kafka message
type RecordValue ccloud.RecordValue

// CreateTopic creates a topic using the Admin Client API
func CreateTopic(p *kafka.Producer, topic string) {

	a, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		os.Exit(1)
	}
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		fmt.Printf("ParseDuration(60s): %s", err)
		os.Exit(1)
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 3}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Admin Client request error: %v\n", err)
		os.Exit(1)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Failed to create topic: %v\n", result.Error)
			os.Exit(1)
		}
		fmt.Printf("%v\n", result)
	}
	a.Close()

}

func buildSignal(long string, lat string, speed string, seq int) (*RecordValue) {
	TimestampSignal := time.Now().UnixNano() / int64(time.Millisecond)
	l1, _ := strconv.ParseFloat(long, 64)
	l2, _ := strconv.ParseFloat(lat, 64)
	// ig, _ := strconv.ParseBool(ign)
	sp, _ := strconv.Atoi(speed)
	return &RecordValue{
		Longitude: l1,
		Satellite_count: 0,
		External_power_voltage: 0,
		Version: 5,
		Peername: "192.168.218.69:59457",
		Timestamp: TimestampSignal,
		Thing_id: "mrn:thing:gps:quec001-444444444411122",
		Id: "mrn:thing:gps:quec001-444444444411122:signal:" + ksuid.New().String(),
		Hdop: 1,
		Inputs: 255,
		Protocol: "quec001",	
		Received_timestamp: TimestampSignal,
		Ignition_status: true,
		Speed: sp,
		Carrier: 7,
		Seq_num: seq,
		Fix_timestamp: TimestampSignal,
		Latitude: l2,
		Horometer: 2711437,
		Temperature: 30,
		Raw: "K1JFU1A6R1RGUkksMDQ4MDBCLDY2NjY2Nzc3Nzc4ODg4OSxndjIwMCwsMTAsMSwxLDQwLjAsMTcwLDE5LjUsLTYwLjY4MDY4NSwtMzIuOTM5Mzk0LDIwMjEwOTE0MTc0MjQzLDA3MjIsMDAwNywxMTQ2LDA0MDQsMDAsMzMwLjIsMDA3NTM6MTA6MzcsMjJGRjAsMSwsLCwsLDIwMjEwOTE0MTc0MjQzLDAwMEIk",
		Device_id: "444444444411122",
		Ingestion_timestamp: TimestampSignal,
		Altitude: 19.5,
		Main_power_on: false,
		Odometer: 330000,
		Battery_level: 3,
		Event_code: 222,
		Fix_status: 0,
		Gprs_status: 0,
		Is_buffered: false,
		Rssi: 0,
		Heading: 170,
		Index: 0}
}

func main() {

	// Initialization
	configFile, topic := ccloud.ParseArgs()
	conf := ccloud.ReadCCloudConfig(*configFile)

	// Create Producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": conf["bootstrap.servers"]})
		// "sasl.mechanisms": conf["sasl.mechanisms"],
		// "security.protocol": conf["security.protocol"]
		// "sasl.username":     conf["sasl.username"],
		// "sasl.password":     conf["sasl.password"]})
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Create topic if needed
	// CreateTopic(p, *topic)

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	points := [][]string{{"-58.38160514831543", "-34.60039746004035", "50"},
						{"-58.38126182556152", "-34.60604926956928", "60"},
						{"-58.38074684143066", "-34.61360796350612", "70"},
						{"-58.38074684143066", "-34.6171045096553", "80"},
						{"-58.37808609008788", "-34.61745768794486", "90"},
						{"-58.368816375732415", "-34.6169632379187", "100"},
						{"-58.369460105895996", "-34.61053511954633", "110"},
						{"-58.3701467514038", "-34.60304679365", "80"}}

	
	size := len(points)

	for n := 0; n < size; n++ {
		long := points[n][0]
		lat := points[n][1]
		// ign := points[n][2]
		speed := points[n][2]
		recordKey := "alice"
		data := buildSignal(long, lat, speed, n)
		recordValue, _ := json.Marshal(&data)
		fmt.Printf("Preparing to produce record: %s\t%s\n", recordKey, recordValue)
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
			Key:            []byte(recordKey),
			Value:          []byte(recordValue),
		}, nil)
		if n <= (size - 1) {
			time.Sleep(3 * time.Second)
		}
		if n == (size - 2) {
			time.Sleep(180 * time.Second)
		}
	}

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)

	fmt.Printf("10 messages were produced to topic %s!", *topic)

	p.Close()

}
