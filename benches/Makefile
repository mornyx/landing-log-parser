.PHONY: bench
bench:
	go test -bench=^BenchmarkStreamParser$$ -benchtime=10s -count=3 -cpuprofile=cpu.pprof -memprofile=mem.pprof

.PHONY: bench-with-io
bench-with-io:
	go test -bench=^BenchmarkStreamParserWithIO$$ -benchtime=10s -count=3
