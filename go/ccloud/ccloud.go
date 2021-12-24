package ccloud

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
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

// RecordValue represents the struct of the value in a Kafka message
type RecordValue struct {
	Longitude float64	`json:"longitude"`
	Satellite_count int	`json:"satellite_count"`
	External_power_voltage int	`json:"external_power_voltage"`
	Version int	`json:"version"`
	Peername string	`json:"peername"`
	Timestamp int64	`json:"timestamp"`
	Thing_id string	`json:"thing_id"`
	Id string	`json:"id"`
	Hdop int	`json:"hdop"`
	Inputs int	`json:"inputs"`
	Protocol string	`json:"protocol"`
	Received_timestamp int64	`json:"received_timestamp"`
	Ignition_status bool	`json:"ignition_status"`
	Speed int	`json:"speed"`
	Carrier int	`json:"carrier"`
	Seq_num int	`json:"seq_num"`
	Fix_timestamp int64	`json:"fix_timestamp"`
	Latitude float64	`json:"latitude"`
	Horometer int	`json:"horometer"`
	Temperature int	`json:"temperature"`
	Raw string	`json:"raw"`
	Device_id string	`json:"device_id"`
	Ingestion_timestamp int64	`json:"ingestion_timestamp"`
	Altitude float64	`json:"altitude"`
	Main_power_on bool	`json:"main_power_on"`
	Odometer int	`json:"odometer"`
	Battery_level int	`json:"battery_level"`
	Event_code int	`json:"event_code"`
	Fix_status int	`json:"fix_status"`
	Gprs_status int	`json:"gprs_status"`
	Is_buffered bool	`json:"is_buffered"`
	Rssi int	`json:"rssi"`
	Heading int	`json:"heading"`
	Index int	`json:"index"`
}

// ParseArgs parses the command line arguments and
// returns the config file and topic on success, or exits on error
func ParseArgs() (*string, *string) {

	configFile := flag.String("f", "", "Path to Confluent Cloud configuration file")
	topic := flag.String("t", "", "Topic name")
	flag.Parse()
	if *configFile == "" || *topic == "" {
		flag.Usage()
		os.Exit(2) // the same exit code flag.Parse uses
	}

	return configFile, topic

}

// ReadCCloudConfig reads the file specified by configFile and
// creates a map of key-value pairs that correspond to each
// line of the file. ReadCCloudConfig returns the map on success,
// or exits on error
func ReadCCloudConfig(configFile string) map[string]string {

	m := make(map[string]string)

	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m

}
