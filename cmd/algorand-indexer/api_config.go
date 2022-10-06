package main

import (
	"fmt"
	"os"

	"github.com/algorand/indexer/api"
	"github.com/algorand/indexer/config"
	"github.com/spf13/cobra"
)

var (
	suppliedAPIConfigFile string
	showAllDisabled       bool
)

var apiConfigCmd = &cobra.Command{
	Use:   "api-config",
	Short: "api configuration",
	Long:  "api configuration",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		config.BindFlagSet(cmd.Flags())
		err = configureLogger()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to configure logger: %v", err)
			panic(exit{1})
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get swagger: %v", err)
			panic(exit{1})
		}

		var displayDisabledMapConfig *api.DisplayDisabledMap

		output, err := displayDisabledMapConfig.String()

		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to output yaml: %v", err)
			panic(exit{1})
		}

		fmt.Fprint(os.Stdout, output)
		panic(exit{0})

	},
}
