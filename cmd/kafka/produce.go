/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "produce message to kafka",
	Long: `Produce message to kafka via command
	Example:
	app kafka produce --brokers 127.0.0.1:19092 --topic FooTopic  --json '{"topic":"topic_name","message":"message"}'

	Example using file:
	app kafka produce --brokers 127.0.0.1:19092 --topic FooTopic  --jsonfile data/sample1.json
	`,
	Run: publishMessage,
}

func init() {
	KafkaCmd.AddCommand(produceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	produceCmd.Flags().StringP("json", "j", "", "json message")
	produceCmd.Flags().StringP("jsonfile", "f", "", "load json file")
	produceCmd.Flags().StringP("topic", "t", "", "topic name")
	produceCmd.Flags().StringP("brokers", "b", "", "brokers")

}

func publishMessage(cmd *cobra.Command, args []string) {
	brokers := cmd.Flag("brokers").Value.String()
	if brokers == "" {
		fmt.Println("brokers is required")
		return
	}

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RetryBackoffFn(func(int) time.Duration { return time.Second }),
		kgo.RecordRetries(3),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
	}

	broker, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		fmt.Println("failed to create broker")
		return
	}

	dataMap := []map[string]interface{}{}

	jsonMessage := cmd.Flag("json").Value.String()
	if jsonMessage == "" {
		jsonFilePath := cmd.Flag("jsonfile").Value.String()
		if jsonFilePath != "" {
			dat, err := os.ReadFile(jsonFilePath)
			if err != nil {
				fmt.Println("failed to read file")
				return
			}
			jsonMessage = string(dat)
		}
	}

	if jsonMessage != "" {
		if err := json.Unmarshal([]byte(jsonMessage), &dataMap); err != nil {
			fmt.Println("failed to unmarshal json")
			return
		}
	}

	for i, item := range dataMap {
		jsonOut, err := json.Marshal(item)
		if err != nil {
			fmt.Println("failed to marshal json")
			return
		}

		broker.ProduceSync(context.Background(), &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Topic: cmd.Flag("topic").Value.String(),
			Value: jsonOut,
		})

		fmt.Println(string(jsonOut))
	}

}
