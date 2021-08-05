package benches

import (
	"io/ioutil"
	"os"
	"testing"

	logparser "github.com/mornyx/landing-log-parser"
)

func BenchmarkStreamParser(b *testing.B) {
	content, err := ioutil.ReadFile("bench_100k.log")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := logparser.ParseFromBytes(content)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkStreamParserWithIO(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		file, err := os.Open("bench_100k.log")
		if err != nil {
			panic(err)
		}
		parser := logparser.NewStreamParser(file)
		b.StartTimer()
		for {
			entry, err := parser.ParseNext()
			if err != nil {
				panic(err)
			}
			if entry == nil {
				break
			}
		}
	}
}
