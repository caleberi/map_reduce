package main

import (
	"fmt"
	"log"
	"strings"
	"unicode"

	"github.com/caleberi/map_reduce/mr"
)

// A frequency count application "plugin" fo map-reduce
// There is a neeed to allow easy loading of various
// Map reduce function implementation base on use case

// The Map function is called once for each file .
// first argument is the input file and the second
// is the file's content.
func Map(filename string, contents string) []mr.KVPair {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kvpairs := []mr.KVPair{}

	for _, w := range words {
		kvpairs = append(kvpairs, mr.KVPair{Key: w, Value: "1"})
	}
	return kvpairs
}

// The Reduce function is called for each key generated by the map
// tasks with the list of all values created for that key by any
// any map task
func Reduce(key string, values []string) string {
	log.Printf("Reduce: Key: %s, Values: %v", key, values)
	return fmt.Sprintf("%s %d", key, len(values))
}

func main() {}
