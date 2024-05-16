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
	AccessToken string `yaml:"access_token"`
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

	// Extract the access token
	accessToken := tokenData.AccessToken

	// Run the first command to create a new allocation
	newAllocationCmd := exec.Command("./zbox", "newallocation", "--size", "500032385536", "--lock", "100")

	var out bytes.Buffer
	var stderr bytes.Buffer
	newAllocationCmd.Stdout = &out
	newAllocationCmd.Stderr = &stderr

	err = newAllocationCmd.Run()
	if err != nil {
		fmt.Printf("Error creating allocation: %s\n", err)
		printAllLines(stderr.Bytes())
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
	migrateCmd := exec.Command("./s3migration", "migrate", "--allocation", allocationID, "--source", "google_drive", "--access-token", accessToken)

	rawOutput, err := migrateCmd.CombinedOutput()

	var out2 bytes.Buffer
	// var stderr2 bytes.Buffer
	migrateCmd.Stdout = &out2
	// migrateCmd.Stderr = &stderr2

	error_w := migrateCmd.Run()
	if err != nil {
		fmt.Printf("Error running migration: %s\n", error_w)
		fmt.Printf("Error running migration: %s\n", err)
		// printAllLines(stderr2.Bytes())
		return
	}
	fmt.Printf("Output of migrate command: %s\n", rawOutput)

	// Print the output of the migration command
	fmt.Printf("Output of migrate command: %s\n", out2.String())
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
