/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// offsetCmd represents the consumer command
var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "consume manual offset",
	Long: `Consumer message to kafka via command
	Example:
		devkit kafka offset --brokers localhost:9092 --group my-group --topic my-topic --offset 1
	`,
	Run: consumeOffsetMessage,
}

func init() {
	KafkaCmd.AddCommand(offsetCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// offsetCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// offsetCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	offsetCmd.Flags().StringP("group", "g", "", "consumer group")
	offsetCmd.Flags().StringP("topic", "t", "", "topic name")
	offsetCmd.Flags().StringP("brokers", "b", "", "brokers")
	offsetCmd.Flags().IntP("offset", "o", 1, "max poll offset")
	offsetCmd.Flags().StringP("datetime", "d", "", "time format 3/19/2025, 12:06:37")
}

func consumeOffsetMessage(cmd *cobra.Command, args []string) {
	start := time.Now()
	defer func() {
		fmt.Println("elapsed time:", time.Since(start))
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

	offset, err := cmd.Flags().GetInt("offset")
	if err != nil {
		fmt.Println("offset is required")
		return
	}

	dateTimeStr, _ := cmd.Flags().GetString("datetime")

	dateTime := time.Now()
	if dateTimeStr != "" {
		dateTime, err = time.Parse("1/2/2006, 15:04:05", dateTimeStr)
		if err != nil {
			fmt.Println("datetime format is invalid")
			return
		}
	}

	// Configure the Kafka client with consumer group settings
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),                                          // Join the consumer group
		kgo.ConsumeTopics(topic),                                          // Subscribe to the topic
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)), // Optional: for debugging
		kgo.DisableAutoCommit(),
		kgo.DialTimeout(60*time.Second),
		kgo.RequestTimeoutOverhead(60*time.Second),
		kgo.RetryTimeout(11*time.Second), // if updating this, update below's SetTimeoutMillis
		kgo.MetadataMinAge(250*time.Millisecond),
		kgo.ClientID("scm-kit"),
	)
	if err != nil {
		fmt.Println("failed to create client. err:", err)
		return
	}
	defer cl.Close()

	ctx := context.Background()

	adm := kadm.NewClient(cl)
	adm.SetTimeoutMillis(5000)

	var (
		listed   kadm.ListedOffsets
		commitTo kadm.Offsets
	)

	switch offset {
	case -1:
		listed, err = adm.ListEndOffsets(ctx, topic)
	case 1:
		listed, err = adm.ListStartOffsets(ctx, topic)
	default:
		listed, err = adm.ListOffsetsAfterMilli(ctx, dateTime.UnixMilli(), topic)
	}

	if err != nil {
		fmt.Println("failed to list offsets. err:", err)
		return
	}

	commitTo = listed.Offsets()

	committed, err := adm.CommitOffsets(ctx, group, commitTo)
	if err != nil {
		fmt.Println("failed to commit offsets. err:", err)
		return
	}

	fmt.Println("committed offsets:", committed)

}
