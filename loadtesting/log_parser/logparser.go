package logparser

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"

	zlogger "github.com/0chain/s3migration/logger"
	"gopkg.in/yaml.v2"
)

type FileData struct {
	FileID string `yaml:"file_id"`
	Size   string `yaml:"size,omitempty"`
	Time   string `yaml:"time,omitempty"`
}

func parseUploadDone(line string, ds *[]FileData, mutex *sync.Mutex) {
	regexPattern := regexp.MustCompile(`upload done: (\w+) size (\d+)`)
	match := regexPattern.FindStringSubmatch(line)
	if match != nil {
		mutex.Lock()
		defer mutex.Unlock()
		*ds = append(*ds, FileData{FileID: match[1], Size: match[2]})
	} else {
		partsDone := strings.Split(line, "upload done: ")
		partsSize := strings.Split(partsDone[1], " size ")
		if len(partsSize) == 2 {
			mutex.Lock()
			defer mutex.Unlock()
			*ds = append(*ds, FileData{FileID: partsSize[0], Size: partsSize[1]})
		}
	}
}

func parseUploadCompleted(line string, ds *[]FileData, mutex *sync.Mutex) {
	regexPattern := regexp.MustCompile(`Upload completed for (\w+) in ([\dms.]+)`)
	match := regexPattern.FindStringSubmatch(line)
	if match != nil {
		mutex.Lock()
		defer mutex.Unlock()
		*ds = append(*ds, FileData{FileID: match[1], Time: match[2]})
	} else {
		parts_done := strings.Split(line, "Upload completed for ")
		parts_size := strings.Split(parts_done[1], " in ")
		if len(parts_size) == 2 {
			mutex.Lock()
			defer mutex.Unlock()
			*ds = append(*ds, FileData{FileID: parts_size[0], Time: parts_size[1]})

		}
	}
}

func parseLine(line string, ds *[]FileData, mutex *sync.Mutex) {
	if strings.Contains(line, "upload done: ") {
		parseUploadDone(line, ds, mutex)
	} else if strings.Contains(line, "Upload completed for ") {
		parseUploadCompleted(line, ds, mutex)
	}
}

func parse(ds *[]FileData, lines []string, wg *sync.WaitGroup, mutex *sync.Mutex) {
	defer wg.Done()
	for _, line := range lines {
		if strings.Contains(line, "s3-migration [INFO] ") && (strings.Contains(line, "upload done") || strings.Contains(line, "Upload completed for")) {
			line = strings.ReplaceAll(line, "\n", "")
			line = strings.ReplaceAll(line, " <nil>", "")
			parseLine(line, ds, mutex)
		}
	}
}

func mergeData(ds []FileData) []FileData {
	mergedDS := make([]FileData, 0)
	uniqueFile := make(map[string]bool)
	for _, d := range ds {
		if _, ok := uniqueFile[d.FileID]; ok {
			for i, item := range mergedDS {
				if item.FileID == d.FileID {
					if d.Size != "" {
						mergedDS[i].Size = d.Size
					} else if d.Time != "" {
						mergedDS[i].Time = d.Time
					}
					break
				}
			}
		} else {
			mergedDS = append(mergedDS, d)
			uniqueFile[d.FileID] = true
		}
	}
	return mergedDS
}

func Log_parser(file_name string) {
	ds := make([]FileData, 0)
	var mutex sync.Mutex

	file, err := os.Open(file_name)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go parse(&ds, lines, &wg, &mutex) // Fix: Pass the address of mutex

	wg.Wait()

	fmt.Printf("Total lines: %d\n", len(ds))

	data := mergeData(ds)

	yamlData, err := yaml.Marshal(data)
	if err != nil {
		fmt.Println("Error marshaling YAML:", err)
		return
	}

	// file_name_alternative to input file
	file_output := strings.Replace(file_name, ".txt", ".yaml", 1)
	zlogger.Logger.Info("Output file: " + file_output)

	yamlFile, err := os.Create(file_output)
	if err != nil {
		zlogger.Logger.Error("Error creating YAML file: " + err.Error())
		return
	}
	defer yamlFile.Close()

	_, err = yamlFile.Write(yamlData)
	if err != nil {
		zlogger.Logger.Error("Error creating YAML file: " + err.Error())
		return
	}

	zlogger.Logger.Info("Successfully wrote YAML data to file")

}
