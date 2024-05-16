package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"

	"github.com/go-yaml/yaml"
)

type TokenData struct {
	AccessToken    string `yaml:"access_token"`
	AllocationSize string `yaml:"allocation_size"`
}

func main() {
	tokenFile := flag.String("token-file", "token.yaml", "File containing the access token")

	// Parse the flags
	flag.Parse()

	// Open the token file
	file, err := os.Open(*tokenFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Parse the YAML file
	var tokenData TokenData
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&tokenData)
	if err != nil {
		panic(err)
	}

	// Extract the access token and allocation size
	accessToken := tokenData.AccessToken
	allocationSize := tokenData.AllocationSize

	// Run the first command to create a new allocation
	newAllocationCmd := exec.Command("./zbox", "newallocation", "--size", allocationSize, "--lock", "100")

	// Capture the combined output of the command
	rawOutput, err := newAllocationCmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error creating allocation: %s\n", err)
		fmt.Print(string(rawOutput))
		return
	}
	fmt.Printf("Output of newallocation command: %s\n", string(rawOutput))

	// Extract the allocation ID from the output
	output := string(rawOutput)
	re := regexp.MustCompile(`Allocation created: ([a-f0-9]+)`)
	match := re.FindStringSubmatch(output)
	if len(match) < 2 {
		fmt.Println("Failed to extract allocation ID from output")
		return
	}
	allocationID := match[1]
	fmt.Printf("Extracted allocation ID: %s\n", allocationID)

	// Run the second command to migrate using the extracted allocation ID and provided access token
	migrateCmd := exec.Command("./s3migration", "migrate", "--allocation", allocationID, "--source", "google_drive", "--access-token", accessToken)

	// Capture the combined output of the migration command
	rawOutput, err = migrateCmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error running migration: %s\n", err)
		fmt.Print(string(rawOutput))
		return
	}
	fmt.Printf("Output of migrate command: %s\n", string(rawOutput))

	// Run the command to cancel the allocation migration
	cancelAllocationCmd := exec.Command("./zbox", "alloc-cancel", "--allocation", allocationID)

	// Capture the combined output of the cancel command
	rawOutput, err = cancelAllocationCmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error cancelling the allocation migration: %s\n", err)
		fmt.Print(string(rawOutput))

		return
	}
	fmt.Printf("Output of allocation cancel command: %s\n", string(rawOutput))
}
