package main

import (
	"flag"
	"fmt"
	"github.com/vkrasnov/dictator"
	"io/ioutil"
	"log"
	"math"
	"path"
	"runtime"
)

var windowSize = flag.Int("windowsize", 16384, "Window size used by the compression")
var dictSize = flag.Int("dictsize", 16384, "Maximal size of the generated deflate dictionary")
var trainDir = flag.String("in", "", "Path to directory with the training data, mandatory")
var out = flag.String("out", "", "Name of the generated dictionary file, mandatory")
var compLevel = flag.Int("l", 4, "Specify the desired compression level 4-9")
var concurrency = flag.Int("j", runtime.GOMAXPROCS(0), "The maximum number of CPUs to use")

func printUsage() {
	flag.PrintDefaults()
}

func main() {
	flag.Parse()

	if *trainDir == "" || *out == "" || *compLevel < 4 || *compLevel > 9 {
		printUsage()
		return
	}

	files, _ := ioutil.ReadDir(*trainDir)
	var paths []string
	for _, f := range files {
		if f.IsDir() || !f.Mode().IsRegular() {
			continue
		}
		paths = append(paths, path.Join(*trainDir, f.Name()))
	}
	progress := make(chan float64, len(paths))
	go func() {
		for percent := range progress {
			fmt.Printf("\r%.2f%% ", percent)
		}
	}()
	table := dictator.GenerateTable(*windowSize, paths, *compLevel, progress, *concurrency)
	fmt.Println("\r100%  ")
	fmt.Println("Total incompressible strings found: ", len(table))

	dictionary := dictator.GenerateDictionary(table, *dictSize, int(math.Ceil(float64(len(paths)) * 0.01)))

	if err := ioutil.WriteFile(*out, []byte(dictionary), 0644); err != nil {
		log.Fatal("Failed to write dictionary:", err)
	}
}
