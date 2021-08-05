package main

import (
	"encoding/json"
	"fmt"
	"os"

	logparser "github.com/mornyx/landing-log-parser"
)

func main() {
	parser := logparser.NewStreamParser(os.Stdin)
	for {
		entry, err := parser.ParseNext()
		if err != nil {
			panic(err)
		}
		if entry == nil {
			break
		}
		b, _ := json.Marshal(entry)
		fmt.Println(string(b))
	}
}
