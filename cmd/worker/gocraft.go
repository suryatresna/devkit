/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package worker

import (
	"encoding/json"
	"fmt"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/cobra"
)

var gocraftCmd = &cobra.Command{
	Use:   "gocraft",
	Short: "gocraft toolkit",
	Long:  `Mini tool for gocraft usage like triggering worker.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	Run: addWorker,
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.devkit.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	gocraftCmd.Flags().StringP("job", "j", "", "job name worker")
	gocraftCmd.Flags().StringP("ns", "n", "", "name space worker")
	gocraftCmd.Flags().StringP("redis", "r", "", "host redis")
	gocraftCmd.Flags().StringP("pwd", "a", "", "password redis")
	gocraftCmd.Flags().StringP("username", "u", "", "username redis")
	gocraftCmd.Flags().StringP("json", "v", "", "json payload worker")
}

func addWorker(cmd *cobra.Command, args []string) {
	redisHost := cmd.Flag("redis").Value.String()
	if redisHost == "" {
		fmt.Println("redis host is required")
		return
	}

	pwd := cmd.Flag("pwd").Value.String()
	username := cmd.Flag("username").Value.String()

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
			if pwd != "" && username != "" {
				return redis.Dial("tcp", redisHost, redis.DialPassword(pwd), redis.DialUsername(username))
			}
			return redis.Dial("tcp", redisHost)
		},
	}

	var enqueuer = work.NewEnqueuer(ns, redisPool)

	_, err := enqueuer.Enqueue(job, workQ)
	if err != nil {
		panic(err)
	}
}
