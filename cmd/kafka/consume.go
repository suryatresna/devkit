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
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// consumeCmd represents the consumer command
var consumeCmd = &cobra.Command{
	Use:   "consumer",
	Short: "consume manual message commit",
	Long: `Consumer message to kafka via command
	`,
	Run: consumeMessage,
}

func init() {
	KafkaCmd.AddCommand(consumeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	consumeCmd.Flags().StringP("group", "g", "", "consumer group")
	consumeCmd.Flags().StringP("topic", "t", "", "topic name")
	consumeCmd.Flags().StringP("brokers", "b", "", "brokers")
	consumeCmd.Flags().StringP("poll", "o", "1", "max poll offset")

}

func consumeMessage(cmd *cobra.Command, args []string) {
	start := time.Now()
	defer func() {
		fmt.Println("elapsed time:", time.Since(start))
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		os.Exit(1)
		fmt.Println("interrupted, elapsed time: ", time.Since(start))
	}()

	brokers := cmd.Flag("brokers").Value.String()
	if brokers == "" {
		fmt.Println("brokers is required")
		return
	}

	group := cmd.Flag("group").Value.String()

	topic := cmd.Flag("topic").Value.String()

	pollStr := cmd.Flag("poll").Value.String()
	poll, err := strconv.Atoi(pollStr)
	if err != nil {
		fmt.Println("poll format is invalid")
		return
	}

	seeds := kgo.SeedBrokers(strings.Split(brokers, ",")...)

	var adm *kadm.Client
	{
		cl, err := kgo.NewClient(seeds)
		if err != nil {
			fmt.Println("failed to create client")
			return
		}
		adm = kadm.NewClient(cl)
	}

	os, err := adm.FetchOffsetsForTopics(context.Background(), group, topic)
	if err != nil {
		fmt.Println("failed to fetch offsets")
		return
	}

	cl, err := kgo.NewClient(seeds, kgo.ConsumePartitions(os.KOffsets()))
	if err != nil {
		fmt.Println("failed to create client")
		return
	}
	defer cl.Close()

	fmt.Println("Waiting for one record...")
	fs := cl.PollRecords(context.Background(), poll)

	if err := adm.CommitAllOffsets(context.Background(), group, kadm.OffsetsFromFetches(fs)); err != nil {
		fmt.Println("failed to commit offsets")
		return
	}

	r := fs.Records()[0]
	fmt.Printf("Successfully committed record on partition %d at offset %d!\n", r.Partition, r.Offset)

}
