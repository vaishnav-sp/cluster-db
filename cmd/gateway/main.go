package main

import (
	"fmt"
	"os"

	"github.com/vaishnav-sp/cluster-db/internal/app"
)

var Version = "dev"

func main() {
	application, err := app.New(Version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize application: %v\n", err)
		os.Exit(1)
	}

	if err := application.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Application error: %v\n", err)
		os.Exit(1)
	}
}
