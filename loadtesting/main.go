package main

import (
	"bytes"
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

	//
	// Extract the access token
	accessToken := tokenData.AccessToken
	allocationSize := tokenData.AllocationSize

	// Run the first command to create a new allocation
	newAllocationCmd := exec.Command("./zbox", "newallocation", "--size", allocationSize, "--lock", "100")

	var out bytes.Buffer
	var stderr bytes.Buffer
	newAllocationCmd.Stdout = &out
	newAllocationCmd.Stderr = &stderr

	rawOutput, err := newAllocationCmd.CombinedOutput()

	if err != nil {
		fmt.Printf("Error creating allocation: %s\n", err)
		printAllLines(stderr.Bytes())
		return
	}
	fmt.Printf("Output of migrate command: %s\n", rawOutput)

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
	migrateCmd := exec.Command("./s3migration", "migrate", "--allocation", allocationID, "--source", "google_drive", "--access-token", accessToken)

	rawOutput, err = migrateCmd.CombinedOutput()

	if err != nil {
		fmt.Printf("Error running migration: %s\n", err)
	}
	fmt.Printf("Output of migrate command: %s\n", rawOutput)

	cancelAllocationCmd := exec.Command("./zbox", "alloc-cancel", "--allocation", allocationID)

	rawOutput, err = cancelAllocationCmd.CombinedOutput()

	if err != nil {
		fmt.Printf("Error cancelling the allocation migration: %s\n", err)
	}
	fmt.Printf("Output of allocation cancel command: %s\n", rawOutput)

}

// printAllLines prints all lines from the given byte slice
func printAllLines(data []byte) {
	scanner := bytes.NewBuffer(data)
	for {
		line, err := scanner.ReadString('\n')
		if err != nil {
			break
		}
		fmt.Print(line)
	}
}
