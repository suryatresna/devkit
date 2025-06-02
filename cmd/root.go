/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/suryatresna/devkit/cmd/kafka"
	"github.com/suryatresna/devkit/cmd/sql"
	gocraft "github.com/suryatresna/devkit/cmd/worker"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "devkit",
	Short: "devkit is a toolkit for developer",
	Long: `
devkit is a toolkit for developer, it's a mini tool for kafka usage like produce or consume message and triggering worker on gocraft.
	`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.devkit.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	rootCmd.AddCommand(kafka.KafkaCmd)
	rootCmd.AddCommand(gocraft.WorkerCmd)
	rootCmd.AddCommand(sql.SqlCmd)
}
