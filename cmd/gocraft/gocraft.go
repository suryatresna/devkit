/*
Copyright © 2024 Surya Tresna <surya.tresna@gmail.com>
*/
package gocraft

import (
	"github.com/spf13/cobra"
)

var GocraftCmd = &cobra.Command{
	Use:   "gocraft",
	Short: "gocraft toolkit",
	Long:  `Mini tool for gocraft usage like triggering worker.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.devkit.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	GocraftCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
