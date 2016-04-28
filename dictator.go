package main

import (
	"container/heap"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
)

var windowSize = flag.Int("windowsize", 16384, "Window size")

const (
	maxMatchLength = 258
	minMatchLength = 4
	hashBits       = 15
	hashSize       = 1 << hashBits
	hashMask       = (1 << hashBits) - 1
)

type compressionLevel struct {
	good, lazy, nice, chain int
}

var levels = []compressionLevel{
	{}, // 0
	{3, 0, 8, 4},
	{3, 0, 16, 8},
	{3, 0, 32, 32},
	{4, 4, 16, 16},
	{8, 16, 32, 32},
	{8, 16, 128, 128},
	{8, 32, 128, 256},
	{32, 128, 258, 1024},
	{32, 258, 258, 4096},
}

type dictator struct {
	// Pseudo deflate variables, we need those to perform deflate like matching, to identify strings that are emmited as is
	compressionLevel
	window   []byte
	hashHead [32768]int
	hashPrev []int
	// Accumulate characters emitted as is
	stringBuf []byte
	stringLen int
	// Count all the strings here
	table map[string]int
}

func NewDictator(windowSize int) *dictator {
	dictator := new(dictator)
	dictator.hashPrev = make([]int, windowSize);
	dictator.stringBuf = make([]byte, windowSize);
	return dictator
}

func (d *dictator) init(level int) (err error) {
	if level < 4 || level > 9 {
		return fmt.Errorf("Only supposts levels [4, 9], got %d", level)
	}

	d.compressionLevel = levels[level]
	d.stringLen = 0
	d.table = make(map[string]int)
	for i := range d.hashHead {
		d.hashHead[i] = -1
	}
	return nil
}

// Try to find a match starting at index whose length is greater than prevSize.
// We only look at chainCount possibilities before giving up.
func (d *dictator) findMatch(pos int, prevHead int, prevLength int, lookahead int) (length, offset int, ok bool) {

	minMatchLook := maxMatchLength
	if lookahead < minMatchLook {
		minMatchLook = lookahead
	}

	win := d.window

	// We quit when we get a match that's at least nice long
	nice := len(win) - pos

	if d.nice < nice {
		nice = d.nice
	}

	// If we've got a match that's good enough, only look in 1/4 the chain.
	tries := d.chain
	length = prevLength
	if length >= d.good {
		tries >>= 2
	}

	w0 := win[pos]
	w1 := win[pos+1]
	wEnd := win[pos+length]

	for i := prevHead; tries > 0; tries-- {
		if w0 == win[i] && w1 == win[i+1] && wEnd == win[i+length] {
			n := 2
			for pos+n < len(win) && win[i+n] == win[pos+n] {
				n++
			}

			if n > length && (n > 3) {
				length = n
				offset = pos - i
				ok = true
				if n >= nice {
					// The match is good enough that we don't try to find a better one.
					break
				}
				wEnd = win[pos+n]
			}
		}
		if i = d.hashPrev[i]; i < 0 {
			break
		}
	}
	return
}

func (d *dictator) findUncompressable(in []byte) {
	d.window = in
	pos := 0
	length := minMatchLength - 1

	for {
		lookahead := len(in) - pos
		if lookahead <= minMatchLength {
			break
		}

		hash := ((int(in[pos]) << 10) ^ (int(in[pos]) << 5) ^ (int(in[pos]))) & hashMask
		hashHead := d.hashHead[hash]
		d.hashPrev[pos] = hashHead
		d.hashHead[hash] = pos

		prevLength := length
		if hashHead >= 0 && prevLength < d.nice {
			if newLength, _, ok := d.findMatch(pos, hashHead, minMatchLength-1, lookahead); ok {
				length = newLength
			}
		}

		// Now deflate would output the previous match, therefore if accumulated enough uncompressed bytes, "flush" them
		if prevLength >= minMatchLength && length <= prevLength {
			if d.stringLen >= minMatchLength {
				key := string(d.stringBuf[:d.stringLen])
				d.table[key]++
				d.stringLen = 0
			}
			newPos := pos + prevLength - 1
			if newPos >= len(in) {
				break
			}
			pos++
			for pos < newPos {
				hash := ((int(in[pos]) << 10) ^ (int(in[pos]) << 5) ^ (int(in[pos]))) & hashMask
				hashHead := d.hashHead[hash]
				d.hashPrev[pos] = hashHead
				d.hashHead[hash] = pos
				pos++
			}
			length = minMatchLength - 1
			// Or the previous literal
		} else if pos > 0 {
			d.stringBuf[d.stringLen] = d.window[pos-1]
			d.stringLen++
			pos++
		} else {
			pos++
		}
	}
	if d.stringLen > minMatchLength {
		//fmt.Println(string(d.stringBuf[:d.stringLen]))
		d.table[string(d.stringBuf[:d.stringLen])]++
		d.stringLen = 0
	}
}

// An Item is something we manage in a priority queue.
type scoredString struct {
	value string // The value of the item; arbitrary.
	score int    // The priority of the item in the queue.
}

// A PriorityQueue implements heap.Interface and holds scoredStrings
type PriorityQueue []*scoredString

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].score > pq[j].score
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*scoredString)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func PrintUsage() {
	fmt.Printf("Usage: %s [options] dictionary_size input_directory output_file\n", path.Base(os.Args[0]))
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	window := make([]byte, *windowSize)
	d := NewDictator(*windowSize)
	var table map[string]int
	table = make(map[string]int)
	pq := make(PriorityQueue, 0)
	var dictionary string

	if len(flag.Args()) != 3 {
		PrintUsage()
		return
	}

	dictLen, _ := strconv.Atoi(flag.Arg(0))
	path := flag.Arg(1)

	files, _ := ioutil.ReadDir(path)

	percent := float64(0)
	for num, f := range files {
		file, err := os.Open(path + "/" + f.Name()) // For read access.
		if err != nil {
			continue
		}
		count, err := file.Read(window[:len(window)])
		d.init(4)
		// Create a table of all uncompressable strings in the fime
		d.findUncompressable(window[:count])
		// Merge with the main table
		for k, _ := range d.table {
			table[k]++
		}

		file.Close()

		if newPercent := float64(num) / float64(len(files)) * 100; (newPercent - percent) >= 1 {
			percent = math.Floor(newPercent)
			fmt.Printf("\r%.2f%% ", newPercent)
		}
	}
	fmt.Println("\r100%%")
	fmt.Println("Total uncompressible strings found: ", len(table))
	// If a string appeares in less than 1% of the files, it is probably useless
	threshold := int(math.Ceil(float64(len(files)) * 0.01))
	// Remove unique strings, score others and put into a heap
	heap.Init(&pq)
	for i, v := range table {
		if v < threshold {
			delete(table, i)
		} else {
			item := &scoredString{i, (v * (len(i) - 3)) / len(i)}
			heap.Push(&pq, item)
		}
	}
	fmt.Println("Uncompressible strings with frequency greater than ", threshold, ": ", len(table))

	// Start poping strings from the heap. We want the highest scoring closer to the end, so they are encoded with smaller distance value
	for (pq.Len() > 0) && (len(dictionary) < dictLen) {
		item := heap.Pop(&pq).(*scoredString)
		// Ignore strings that already made it to the dictionary, append others in front
		if !strings.Contains(dictionary, item.value) {
			dictionary = item.value + dictionary
		}
	}
	// Truncate
	if len(dictionary) > dictLen {
		dictionary = dictionary[len(dictionary)-dictLen:]
	}
	// Write
	ioutil.WriteFile(flag.Arg(2), []byte(dictionary), 0644)
}
