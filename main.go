package main

import (
	"github.com/0chain/s3migration/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}
