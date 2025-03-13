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
	// Add check for group presence
	if group == "" {
		fmt.Println("group is required")
		return
	}

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
			fmt.Println("failed to create client. err ", err)
			return
		}
		adm = kadm.NewClient(cl)
	}

	// os, err := adm.FetchOffsetsForTopics(context.Background(), group, topic)
	// if err != nil {
	// 	fmt.Println("failed to fetch offsets. err ", err)
	// 	return
	// }

	cl, err := kgo.NewClient(seeds,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic), // replaced direct-partition consuming option
	)
	if err != nil {
		fmt.Println("failed to create client. err ", err)
		return
	}
	defer cl.Close()

	fmt.Printf("Waiting for %d record...\n", poll)
	fs := cl.PollRecords(context.Background(), poll)

	if err := adm.CommitAllOffsets(context.Background(), group, kadm.OffsetsFromFetches(fs)); err != nil {
		fmt.Println("failed to commit offsets. err ", err)
		return
	}

	records := fs.Records()

	r := records[len(records)-1]
	fmt.Printf("Successfully committed record on partition %d at offset %d!\n", r.Partition, r.Offset)

	for i, record := range records {
		fmt.Printf("record:%d: %s\n", i, string(record.Value))
	}

}
