package main

import (
	"flag"
	"fmt"
	"github.com/vkrasnov/dictator"
	"io/ioutil"
	"math"
)

var windowSize = flag.Int("windowsize", 16384, "Window size used by the compression")
var dictSize = flag.Int("dictsize", 16384, "Maximal size of the generated deflate dictionary")
var trainDir = flag.String("in", "", "Path to directory with the training data, mandatory")
var out = flag.String("out", "", "Name of the generated dictionary file, mandatory")
var compLevel = flag.Int("l", 4, "Specify the desired compression level 4-9")

func PrintUsage() {
	flag.PrintDefaults()
}

func main() {
	flag.Parse()

	if *trainDir == "" || *out == "" || *compLevel < 4 || *compLevel > 9 {
		PrintUsage()
		return
	}

	files, _ := ioutil.ReadDir(*trainDir)
	var paths []string
	for _, f := range files {
		paths = append(paths, *trainDir + "/" + f.Name())
	}
	progress := make(chan float64, len(paths))
	go func() {
		for percent := range progress {
			fmt.Printf("\r%.2f%% ", percent)
		}
	}()
	table := dictator.GenerateTable(*windowSize, paths, *compLevel, progress)
	fmt.Println("\r100%  ")
	fmt.Println("Total incompressible strings found: ", len(table))

	dictionary := dictator.GenerateDictionary(table, *dictSize, int(math.Ceil(float64(len(paths)) * 0.01)))

	ioutil.WriteFile(*out, []byte(dictionary), 0644)
}
