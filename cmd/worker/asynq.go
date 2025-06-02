/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package worker

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"github.com/spf13/cobra"
)

var asynqCmd = &cobra.Command{
	Use:   "asynq",
	Short: "asynq toolkit",
	Long:  `Mini tool for asynq usage like triggering worker.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	Run: addWorkerAsynq,
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.devkit.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	asynqCmd.Flags().StringP("job", "j", "", "job name worker")
	asynqCmd.Flags().StringP("queue", "q", "", "name queue worker")
	asynqCmd.Flags().StringP("redis", "r", "", "host redis")
	asynqCmd.Flags().StringP("json", "v", "", "json payload worker")
}

func addWorkerAsynq(cmd *cobra.Command, args []string) {
	flag := cmd.Flags()

	redisQry, err := flag.GetString("redis")
	if err != nil {
		fmt.Println("error get redis connection")
		return
	}

	// init redis connection
	redisOpt, err := redis.ParseURL(redisQry)
	if err != nil {
		fmt.Println("error parse redis connection")
		return
	}

	asynqClient := asynq.NewClient(asynq.RedisClientOpt{
		Addr:     redisOpt.Addr,
		Network:  redisOpt.Network,
		Username: redisOpt.Username,
		Password: redisOpt.Password,
	})

	defer asynqClient.Close()

	jsonPayload := cmd.Flag("json").Value.String()
	if jsonPayload == "" {
		fmt.Println("json payload is required")
		return
	}

	jsonByte := []byte(jsonPayload)

	job := cmd.Flag("job").Value.String()
	queue := cmd.Flag("queue").Value.String()

	fmt.Printf("Enqueuing task with job: %s, queue: %s, payload: %s\n", job, queue, jsonByte)

	task := asynq.NewTask(
		job,
		jsonByte,
		asynq.Queue(queue),
	)

	info, err := asynqClient.Enqueue(task, asynq.Timeout(24*time.Hour))
	if err != nil {
		fmt.Println("failed to enqueue task:", err)
		return
	}

	fmt.Printf("Task enqueued successfully: ID=%s, Type=%s, Queue=%s\n", info.ID, info.Type, info.Queue)

}
