package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

//Return the min of two integers
func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {

	//Read the file
	file, err := ioutil.ReadFile(path)
	checkError(err)
	fileContent := strings.ToLower(string(file))

	//File cleanup
	re := regexp.MustCompile(`[^0-9a-z\s]`)
	fileContent = re.ReplaceAllString(fileContent, "")

	//Create Hashmap of words (Map)
	wordMap := make(map[string]int)
	for _, word := range strings.Fields(fileContent) {
		wordMap[word] += 1
	}

	//Create Word Count Structure (Reduce)
	var wCount []WordCount
	for word, count := range wordMap {
		if len(word) >= charThreshold {
			wCount = append(wCount, WordCount{word, count})
		}
	}
	sortWordCounts(wCount)
	minSlice := min(numWords, len(wCount))
	return wCount[:minSlice]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
