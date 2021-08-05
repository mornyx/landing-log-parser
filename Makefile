.PHONY: build
build:
	go build -o target/log-parser cmd/log-parser/main.go

.PHONY: test
test:
	go test ./... -coverprofile=cover.out
	go tool cover -func=cover.out -o cover.txt
	go tool cover -html=cover.out -o cover.html
