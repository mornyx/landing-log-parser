# landing-log-parser

Landing Project - A [Unified Log Format](https://github.com/tikv/rfcs/blob/master/text/0018-unified-log-format.md) parser.

# Bench

```text
go test -bench=^BenchmarkStreamParser$ -benchtime=10s -count=3 -cpuprofile=cpu.pprof -memprofile=mem.pprof
goos: darwin
goarch: amd64
pkg: github.com/mornyx/landing-log-parser/benches
cpu: Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz
BenchmarkStreamParser-8             3622           3267617 ns/op
BenchmarkStreamParser-8             3637           3292152 ns/op
BenchmarkStreamParser-8             3637           3322385 ns/op
```
