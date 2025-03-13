/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// commitCmd represents the consumer command
var commitCmd = &cobra.Command{
	Use:   "commit",
	Short: "consume manual commit",
	Long: `Consumer message to kafka via command
	Example:
		devkit kafka commit --brokers localhost:9092 --group my-group --topic my-topic --poll 1
	`,
	Run: consumeMessage,
}

func init() {
	KafkaCmd.AddCommand(commitCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// commitCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// commitCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	commitCmd.Flags().StringP("group", "g", "", "consumer group")
	commitCmd.Flags().StringP("topic", "t", "", "topic name")
	commitCmd.Flags().StringP("brokers", "b", "", "brokers")
	commitCmd.Flags().IntP("poll", "o", 1, "max poll offset")
	commitCmd.Flags().BoolP("skip-log", "l", false, "skip log")
	commitCmd.Flags().IntP("concurrency", "c", 100, "concurrency")
}

func consumeMessage(cmd *cobra.Command, args []string) {
	start := time.Now()
	defer func() {
		fmt.Println("elapsed time:", time.Since(start))
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-c
		cancel()
		fmt.Println("interrupted, elapsed time: ", time.Since(start))
	}()

	brokers := cmd.Flag("brokers").Value.String()
	if brokers == "" {
		fmt.Println("brokers is required")
		return
	}

	group := cmd.Flag("group").Value.String()
	if group == "" {
		fmt.Println("group is required")
		return
	}

	topic := cmd.Flag("topic").Value.String()
	if topic == "" {
		fmt.Println("topic is required")
		return
	}

	pollStr := cmd.Flag("poll").Value.String()
	poll, err := strconv.Atoi(pollStr)
	if err != nil {
		fmt.Println("poll format is invalid")
		return
	}

	concurrencyStr := cmd.Flag("concurrency").Value.String()
	concurrency, err := strconv.Atoi(concurrencyStr)
	if err != nil {
		fmt.Println("concurrency format is invalid")
		return
	}

	skipLog, _ := strconv.ParseBool(cmd.Flag("skip-log").Value.String())

	// Configure the Kafka client with consumer group settings
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),                                          // Join the consumer group
		kgo.ConsumeTopics(topic),                                          // Subscribe to the topic
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)), // Optional: for debugging
	)
	if err != nil {
		fmt.Println("failed to create client. err:", err)
		return
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	fmt.Printf("[INFO] Waiting for %d record(s)...\n", poll)
	totalRecords := int64(0)

	pool := pond.NewPool(concurrency)
	// Poll until we get the desired number of records or context is canceled
	for totalRecords < int64(poll) {
		select {
		case <-ctx.Done():
			return
		default:
			pool.Submit(func() {
				fetches := cl.PollRecords(ctx, 10000)
				if fetches.IsClientClosed() {
					return
				}

				// Handle errors if any
				if errs := fetches.Errors(); len(errs) > 0 {
					for _, err := range errs {
						fmt.Printf("[ERR] poll error: %v\n", err)
					}
					return
				}

				// Process fetched records
				records := fetches.Records()
				if len(records) == 0 {
					return
				}

				totalRecords = atomic.AddInt64(&totalRecords, int64(len(records)))
				for i, record := range records {
					if !skipLog {
						fmt.Printf("[INFO] record:%d: %s (partition: %d, offset: %d)\n",
							i, string(record.Value), record.Partition, record.Offset)
					}
				}

				offsets := kadm.OffsetsFromFetches(fetches)
				_, err := adm.CommitOffsets(context.Background(), group, offsets)
				if err != nil {
					fmt.Printf("[ERR] Failed to commit offsets: %v\n", err)
					return
				}

				fmt.Printf("[INFO] Successfully committed %d/%d record(s)!\n", totalRecords, poll)

				if !skipLog {
					lastRecord := records[len(records)-1]
					fmt.Printf("[INFO] Successfully committed record on partition %d at offset %d!\n",
						lastRecord.Partition, lastRecord.Offset)
				}
			})
		}
	}

	pool.StopAndWait()

	if skipLog {
		fmt.Printf("[INFO] Successfully committed %d record(s)!\n", totalRecords)
	}

}
