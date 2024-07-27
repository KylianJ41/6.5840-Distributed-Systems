package mr

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"plugin"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMapReduceIntegration(t *testing.T) {
	files := []string{
		"../main/pg-tom_sawyer.txt",
		"../main/pg-huckleberry_finn.txt",
		"../main/pg-grimm.txt",
	}
	nReduce := 3

	// will get the wordcount map and reduce func
	mapf := Map
	reducef := Reduce

	coordinator := MakeCoordinator(files, nReduce, true)

	for i := 0; i < 5; i++ {
		go Worker(mapf, reducef)
	}

	if !WaitForCompletion(t, coordinator) {
		return
	}

	verifyOutput(t, nReduce, files)
}

func verifyOutput(t *testing.T, nReduce int, inputFiles []string) {
	expected := make(map[string]int)

	// Generate expected word counts
	for _, filename := range inputFiles {
		content, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("Cannot read file %v: %v", filename, err)
		}
		kva := Map(filename, string(content))
		for _, kv := range kva {
			expected[kv.Key]++
		}
	}

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%d", i)
		file, err := os.Open(filename)
		if err != nil {
			t.Fatalf("Failed to open file %s: %v", filename, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 2)
			if len(parts) != 2 {
				t.Fatalf("Invalid line in output %v", scanner.Text())
			}
			word := parts[0]
			count, err := strconv.Atoi(parts[1])
			if err != nil {
				t.Fatalf("Invalid count in output %v", parts[1])
			}
			if expected[word] != count {
				t.Errorf("Mismatch for word '%s': expected %d, got %d", word, expected[word], count)
			}
			delete(expected, word)
		}
	}

	if len(expected) > 0 {
		t.Errorf("%d words were not in the output", len(expected))
		// Print a few missing words for debugging
		i := 0
		for word := range expected {
			if i < 10 {
				t.Logf("Missing word: %s", word)
				i++
			} else {
				break
			}
		}
	}

	t.Log("Output verification completed successfully")
}

func WaitForCompletion(t *testing.T, coordinator *Coordinator) bool {
	timeout := time.After(10 * time.Second)
	for !coordinator.Done() {
		select {
		case <-timeout:
			t.Fatal("Test timed out")
			return false
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	return true
}

// Updated function to load the word count plugin
func loadWCPlugin() (func(string, string) []KeyValue, func(string, []string) string) {
	pluginFile := filepath.Join(".", "wc.so")

	p, err := plugin.Open(pluginFile)
	if err != nil {
		log.Fatalf("Cannot load plugin %v: %v", pluginFile, err)
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("Cannot find Map in %v: %v", pluginFile, err)
	}
	mapf := xmapf.(func(string, string) []KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("Cannot find Reduce in %v: %v", pluginFile, err)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
