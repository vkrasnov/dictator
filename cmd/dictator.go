package main

import (
	"flag"
	"fmt"
	"github.com/vkrasnov/dictator"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
)

var windowSize = flag.Int("windowsize", 16384, "Window size used by the compression")
var dictSize = flag.Int("dictsize", 16384, "Maximal size of the generated deflate dictionary")
var trainDir = flag.String("in", "", "Path to directory with the training data, mandatory")
var out = flag.String("out", "", "Name of the generated dictionary file, mandatory")
var compLevel = flag.Int("l", 4, "Specify the desired compression level 4-9 for gzip, 10 indicates brotli")
var concurrency = flag.Int("j", runtime.GOMAXPROCS(0), "The maximum number of CPUs to use")
var recursive = flag.Bool("r", false, "Recurse through directories")
var threshold = flag.String("threshold", "0.1%", "The minimum number of occurances for a string to be included in a dictionary. Accepts an absolute value (e.g. 1, 3.0) or a percentage computed over the number of files (e.g. 0.1%, 40%)")

func printUsage() {
	flag.PrintDefaults()
}

func addPaths(dirName string, recursive bool) []string {
	files, _ := ioutil.ReadDir(dirName)
	var paths []string

	for _, f := range files {

		if !f.Mode().IsRegular() {
			if recursive && f.Mode()>>9 == os.ModeDir>>9 { // check if simple directory
				paths = append(paths, addPaths(path.Join(dirName, f.Name()), recursive)...)
			}
			continue
		}

		paths = append(paths, path.Join(dirName, f.Name()))
	}

	return paths
}

func main() {

	flag.Parse()

	if *trainDir == "" || *out == "" || *compLevel < 4 || *compLevel > 10 {
		printUsage()
		return
	}


	t, err := strconv.ParseFloat(strings.TrimSuffix(*threshold, "%"), 64)
	if err != nil {
		printUsage()
		return
	}

	paths := addPaths(*trainDir, *recursive)
	progress := make(chan float64)

	go func() {
		for percent := range progress {
			fmt.Printf("\r%.2f%% ", percent)
		}
		fmt.Println("\r100%   ")
	}()

	table := dictator.GenerateTable(*windowSize, paths, *compLevel, progress, *concurrency)

	if strings.HasSuffix(*threshold, "%") {
		t = float64(len(paths)) * t / 100
	}
	t = math.Ceil(t)

	fmt.Println("\r100%  ")
	fmt.Println("Total incompressible strings found: ", len(table))

	dictionary := dictator.GenerateDictionary(table, *dictSize, int(t))

	if err := ioutil.WriteFile(*out, []byte(dictionary), 0644); err != nil {
		log.Fatal("Failed to write dictionary:", err)
	}
}
