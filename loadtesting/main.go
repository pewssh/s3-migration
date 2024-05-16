package main

import (
	"bytes"
	"flag"
	"fmt"
	"os/exec"
	"regexp"
)

func main() {
	// Define and parse command-line flags
	accessToken := flag.String("access-token", "", "Access token for the migration command")
	flag.Parse()

	if *accessToken == "" {
		fmt.Println("Access token is required")
		return
	}

	// Run the first command to create a new allocation
	newAllocationCmd := exec.Command("./zbox", "newallocation", "--size", "500032385536", "--lock", "100")

	var out bytes.Buffer
	var stderr bytes.Buffer
	newAllocationCmd.Stdout = &out
	newAllocationCmd.Stderr = &stderr

	err := newAllocationCmd.Run()
	if err != nil {
		fmt.Printf("Error creating allocation: %s\n", err)
		fmt.Printf("Stderr: %s\n", stderr.String())
		return
	}

	// Extract the allocation ID from the output
	output := out.String()
	fmt.Printf("Output of newallocation command: %s\n", output)

	re := regexp.MustCompile(`Allocation created: ([a-f0-9]+)`)
	match := re.FindStringSubmatch(output)
	if len(match) < 2 {
		fmt.Println("Failed to extract allocation ID from output")
		return
	}
	allocationID := match[1]
	fmt.Printf("Extracted allocation ID: %s\n", allocationID)

	// Run the second command to migrate using the extracted allocation ID and provided access token
	migrateCmd := exec.Command("./s3migration", "migrate", "--allocation", allocationID, "--source", "google_drive", "--access-token", *accessToken)

	var out2 bytes.Buffer
	var stderr2 bytes.Buffer
	migrateCmd.Stdout = &out2
	migrateCmd.Stderr = &stderr2

	err = migrateCmd.Run()
	if err != nil {
		fmt.Printf("Error running migration: %s\n", err)
		fmt.Printf("Stderr: %s\n", stderr2.String())
		return
	}

	// Print the output of the migration command
	fmt.Printf("Output of migrate command: %s\n", out2.String())
}
