/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package worker

import (
	"github.com/spf13/cobra"
)

// WorkerCmd represents the produce command
var WorkerCmd = &cobra.Command{
	Use:   "worker",
	Short: "add triggering worker on gocraft",
	Long: `Triggering worker on gocraft via command 
	Example:
	app worker gocraft --redis 127.0.0.1:6379 --ns FooNameSpace  --job 'FooJob'
	`,
}

func init() {

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	WorkerCmd.AddCommand(gocraftCmd)
	WorkerCmd.AddCommand(asynqCmd)

}
