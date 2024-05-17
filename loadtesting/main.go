package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/go-yaml/yaml"
)

func main() {
	run_command := flag.String("command", "", "Command for the Google Drive API Testing")

	token := flag.String("token", "", "Access token for the Google Drive API")
	size := flag.String("size", "", "Size of the allocation")
	create := flag.String("create", "", "Create allocation if required")

	flag.Parse()
	fmt.Println("Command: ", *run_command)
	switch *run_command {
	case "cancelalloc":
		cancelAlloc()
	case "run_test":
		runTest(token, size, create)
	}
}

func cancelAlloc() {
	listCommand := exec.Command("./zbox", "listallocations")

	// Capture the combined output of the command
	rawOutput, err := listCommand.CombinedOutput()
	if err != nil {
		fmt.Printf("Error listing allocations: %s\n", err)
		return
	}
	lines := strings.Split(strings.TrimSpace(string(rawOutput)), "\n")

	// Get the last line
	lastLine := lines[len(lines)-1]

	// Extract the allocation ID from the last line
	lastLineParts := strings.Split(lastLine, " |")[0]
	allocationID := strings.TrimSpace(lastLineParts)
	fmt.Printf("Last allocation created ID: %s\n", allocationID)

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

type TokenData struct {
	AccessToken    string `yaml:"access_token"`
	AllocationSize string `yaml:"allocation_size"`
}

func runTest(token *string, size *string, create *string) {
	var rawOutput []byte
	var err error
	var allocationID string

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
	var accessToken string
	// Extract the access token and allocation size
	if *token == "" {
		fmt.Println("Access token not provided")
		accessToken = tokenData.AccessToken
	} else {
		accessToken = *token
	}

	if create == nil {
		fmt.Println("Create allocation not provided")
		return
	}
	// Check if the allocation needs to be created
	if *create == "true" {
		fmt.Println("Creating new allocation")
		var allocationSize string
		if *size == "" {
			fmt.Println("Allocation size not provided")
			allocationSize = tokenData.AllocationSize
		} else {
			allocationSize = *size
		}

		// Run the first command to create a new allocation
		newAllocationCmd := exec.Command("./zbox", "newallocation", "--size", allocationSize, "--lock", "100")

		// Capture the combined output of the command
		rawOutput, err = newAllocationCmd.CombinedOutput()
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
		allocationID = match[1]
	} else {
		// List the allocations
		listCommand := exec.Command("./zbox", "listallocations")

		rawOutput, err = listCommand.CombinedOutput()
		if err != nil {
			fmt.Printf("Error listing allocations: %s\n", err)
			return
		}
		lines := strings.Split(strings.TrimSpace(string(rawOutput)), "\n")

		lastLine := lines[len(lines)-1]

		// Extract the allocation ID from the last line
		lastLineParts := strings.Split(lastLine, " |")[0]
		allocationID = strings.TrimSpace(lastLineParts)
	}
	fmt.Printf("Extracted allocation ID: %s\n", allocationID)

	defer func() {
		if allocationID != "" {
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
	}()

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

}

// ./s3migration migrate --allocation b98cb899d74d68e23cfb4c868d7d1604c563b81cdc1e3cfe20f90582d7d4a5f7   --source google_drive --access-token ya29.a0AXooCgvLlchgmnnuaJc_KMftreV_RARhZXwCM5B2kdFGb4Uz-LMy4EzYlAH0UN1wTwWqFoDUaaRc8zdY0G0biZaHsR_gvToVwP4QR1-3v5vltYRWVnbUG20e1HKPKIY1kKkZuFCBWtWlu8GJFKMPWBxTYDRjyMahHr_uaCgYKAS8SARESFQHGX2MiN85OY3QsyDcVSN8OBWlQzw0171
