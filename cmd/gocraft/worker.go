/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package gocraft

import (
	"encoding/json"
	"fmt"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/cobra"
)

// workerCmd represents the produce command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "add triggering worker on gocraft",
	Long: `Triggering worker on gocraft via command 
	Example:
	app gocraft worker --redis 127.0.0.1:6379 --ns FooNameSpace  --job 'FooJob'
	`,
	Run: addWorker,
}

func init() {
	GocraftCmd.AddCommand(workerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	workerCmd.Flags().StringP("job", "j", "", "job name worker")
	workerCmd.Flags().StringP("ns", "n", "", "name space worker")
	workerCmd.Flags().StringP("redis", "r", "", "host redis")
	workerCmd.Flags().StringP("json", "v", "", "json payload worker")

}

func addWorker(cmd *cobra.Command, args []string) {
	redisHost := cmd.Flag("redis").Value.String()
	if redisHost == "" {
		fmt.Println("redis host is required")
		return
	}

	ns := cmd.Flag("ns").Value.String()
	if redisHost == "" {
		fmt.Println("name space worker is required")
		return
	}

	job := cmd.Flag("job").Value.String()
	if redisHost == "" {
		fmt.Println("job name is required")
		return
	}

	valJson := cmd.Flag("json").Value.String()
	if redisHost == "" {
		fmt.Println("json payload is required")
		return
	}

	mapJson := make(map[string]interface{})
	if err := json.Unmarshal([]byte(valJson), &mapJson); err != nil {
		fmt.Println("failed to parse json payload")
		return
	}

	workQ := work.Q(mapJson)

	var redisPool = &redis.Pool{
		MaxActive: 5,
		MaxIdle:   5,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", redisHost)
		},
	}

	var enqueuer = work.NewEnqueuer(ns, redisPool)

	_, err := enqueuer.Enqueue(job, workQ)
	if err != nil {
		panic(err)
	}
}
