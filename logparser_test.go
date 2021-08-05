package logparser

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogLevel(t *testing.T) {
	var level LogLevel
	var err error
	level, err = StringToLogLevel(LogLevelDebug.String())
	assert.NoError(t, err)
	assert.Equal(t, LogLevelDebug, level)
	level, err = StringToLogLevel(LogLevelInfo.String())
	assert.NoError(t, err)
	assert.Equal(t, LogLevelInfo, level)
	level, err = StringToLogLevel(LogLevelWarn.String())
	assert.NoError(t, err)
	assert.Equal(t, LogLevelWarn, level)
	level, err = StringToLogLevel(LogLevelError.String())
	assert.NoError(t, err)
	assert.Equal(t, LogLevelError, level)
	level, err = StringToLogLevel(LogLevelFatal.String())
	assert.NoError(t, err)
	assert.Equal(t, LogLevelFatal, level)
	level, err = StringToLogLevel("UNKNOWN")
	assert.Error(t, err)
	assert.Equal(t, LogLevel(-1), level)
	assert.Panics(t, func() {
		_ = LogLevel(9999).String()
	})
}

func TestStreamParser_skipChar(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("abc"))
	err := parser.skipChar('a')
	assert.NoError(t, err)
	err = parser.skipChar('x')
	assert.Equal(t, "expect 'x' but found 'b'", err.Error())
	c, _, err := parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, 'c', c)
}

func TestStreamParser_trimChar(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("    abcdeeef"))
	err := parser.trimChar(' ')
	assert.NoError(t, err)
	c, _, err := parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, 'a', c)
	c, _, err = parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, 'b', c)
	c, _, err = parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, 'c', c)
	err = parser.trimChar('d')
	assert.NoError(t, err)
	err = parser.trimChar('e')
	assert.NoError(t, err)
	c, _, err = parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, 'f', c)
	c, _, err = parser.br.ReadRune()
	assert.Equal(t, io.EOF, err)
}

func TestStreamParser_trimNewLines(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("a\nb\nc\r\nd\ne\r\n\n\n\n\r\nf\n"))
	assert.Equal(t, 1, parser.line)
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 1, parser.line)
	assert.NoError(t, parser.skipChar('a'))
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 2, parser.line)
	assert.NoError(t, parser.skipChar('b'))
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 3, parser.line)
	assert.NoError(t, parser.skipChar('c'))
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 4, parser.line)
	assert.NoError(t, parser.skipChar('d'))
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 5, parser.line)
	assert.NoError(t, parser.skipChar('e'))
	assert.NoError(t, parser.trimNewLines())
	assert.Equal(t, 10, parser.line)
	assert.NoError(t, parser.skipChar('f'))
	assert.Equal(t, io.EOF, parser.trimNewLines())
	assert.Equal(t, 11, parser.line)
}

func TestStreamParser_parseDatetime(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("[2021/08/04 12:00:43.128 +08:00] [INFO]"))
	time, err := parser.parseDatetime()
	assert.NoError(t, err)
	assert.Equal(t, 2021, time.Year())
	assert.Equal(t, 8, int(time.Month()))
	assert.Equal(t, 4, time.Day())
	assert.Equal(t, 12, time.Hour())
	assert.Equal(t, 0, time.Minute())
	assert.Equal(t, 43, time.Second())
	assert.Equal(t, 128*1000*1000, time.Nanosecond())
	s, err := parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, " [INFO]", s)
}

func TestStreamParser_parseLogLevel(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("[INFO] [lib.rs:81]"))
	level, err := parser.parseLogLevel()
	assert.NoError(t, err)
	assert.Equal(t, LogLevelInfo, level)
	s, err := parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, " [lib.rs:81]", s)
}

func TestStreamParser_parseFileLine(t *testing.T) {
	parser := NewStreamParser(strings.NewReader(`[lib.rs:81] ["Welcome to TiKV"]`))
	file, line, err := parser.parseFileLine()
	assert.NoError(t, err)
	assert.Equal(t, "lib.rs", file)
	assert.Equal(t, 81, line)
	s, err := parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, ` ["Welcome to TiKV"]`, s)
	parser = NewStreamParser(strings.NewReader(`[<unknown>] ["Welcome to TiKV"]`))
	file, line, err = parser.parseFileLine()
	assert.NoError(t, err)
	assert.Equal(t, "", file)
	assert.Equal(t, 0, line)
	s, err = parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, ` ["Welcome to TiKV"]`, s)
}

func TestStreamParser_parseStringJson(t *testing.T) {
	parser := NewStreamParser(strings.NewReader(`"A \"hacker\"" (another)`))
	s, err := parser.parseStringJson()
	assert.NoError(t, err)
	assert.Equal(t, `A "hacker"`, s)
	s, err = parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, " (another)", s)
}

func TestStreamParser_parseStringLiteral(t *testing.T) {
	parser := NewStreamParser(strings.NewReader(`err="Grpc(RpcFailure(`))
	s, err := parser.parseStringLiteral()
	assert.NoError(t, err)
	assert.Equal(t, "err", s)
	s, err = parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, `="Grpc(RpcFailure(`, s)
	parser = NewStreamParser(strings.NewReader(`"Grpc(RpcFailure(RpcStatus { code: 14-UNAVAILABLE, message: \"failed to connect to all addresses\", details: [] }))"] [endpoints=127.0.0.1:2379]`))
	s, err = parser.parseStringLiteral()
	assert.NoError(t, err)
	assert.Equal(t, `Grpc(RpcFailure(RpcStatus { code: 14-UNAVAILABLE, message: "failed to connect to all addresses", details: [] }))`, s)
	s, err = parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, "] [endpoints=127.0.0.1:2379]", s)
}

func TestStreamParser_parseMessage(t *testing.T) {
	parser := NewStreamParser(strings.NewReader(`[connecting]`))
	msg, err := parser.parseMessage()
	assert.NoError(t, err)
	assert.Equal(t, "connecting", msg)
	parser = NewStreamParser(strings.NewReader(`["connecting to PD endpoint"] [xxx]`))
	msg, err = parser.parseMessage()
	assert.NoError(t, err)
	assert.Equal(t, "connecting to PD endpoint", msg)
	s, err := parser.br.ReadString('\n')
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, " [xxx]", s)
}

func TestStreamParser_parseFields(t *testing.T) {
	parser := NewStreamParser(strings.NewReader("[err=\"Grpc(RpcFailure(RpcStatus { code: 14-UNAVAILABLE, message: \\\"failed to connect to all addresses\\\", details: [] }))\"] [endpoints=127.0.0.1:2379]\n"))
	fields, err := parser.parseFields()
	assert.NoError(t, err)
	assert.Len(t, fields, 2)
	assert.Equal(t, "err", fields[0].Name)
	assert.Equal(t, `Grpc(RpcFailure(RpcStatus { code: 14-UNAVAILABLE, message: "failed to connect to all addresses", details: [] }))`, fields[0].Value)
	assert.Equal(t, "endpoints", fields[1].Name)
	assert.Equal(t, "127.0.0.1:2379", fields[1].Value)
	c, _, err := parser.br.ReadRune()
	assert.NoError(t, err)
	assert.Equal(t, '\n', c)
	_, _, err = parser.br.ReadRune()
	assert.Equal(t, io.EOF, err)
}

func testStreamParserParseNext(t *testing.T, log string) {
	parser := NewStreamParser(strings.NewReader(log))
	entry, err := parser.ParseNext()
	assert.NoError(t, err)
	assert.Equal(t, 2021, entry.Header.DateTime.Year())
	assert.Equal(t, 8, int(entry.Header.DateTime.Month()))
	assert.Equal(t, 4, entry.Header.DateTime.Day())
	assert.Equal(t, 12, entry.Header.DateTime.Hour())
	assert.Equal(t, 0, entry.Header.DateTime.Minute())
	assert.Equal(t, 43, entry.Header.DateTime.Second())
	assert.Equal(t, 128*1000*1000, entry.Header.DateTime.Nanosecond())
	assert.Equal(t, LogLevelInfo, entry.Header.Level)
	assert.Equal(t, "lib.rs", entry.Header.File)
	assert.Equal(t, 81, entry.Header.Line)
	assert.Equal(t, "Welcome to TiKV", entry.Message)
	assert.Len(t, entry.Fields, 0)
	entry, err = parser.ParseNext()
	assert.NoError(t, err)
	assert.Equal(t, 2021, entry.Header.DateTime.Year())
	assert.Equal(t, 8, int(entry.Header.DateTime.Month()))
	assert.Equal(t, 4, entry.Header.DateTime.Day())
	assert.Equal(t, 12, entry.Header.DateTime.Hour())
	assert.Equal(t, 0, entry.Header.DateTime.Minute())
	assert.Equal(t, 43, entry.Header.DateTime.Second())
	assert.Equal(t, 129*1000*1000, entry.Header.DateTime.Nanosecond())
	assert.Equal(t, LogLevelDebug, entry.Header.Level)
	assert.Equal(t, "", entry.Header.File)
	assert.Equal(t, 0, entry.Header.Line)
	assert.Equal(t, "test_message", entry.Message)
	assert.Len(t, entry.Fields, 2)
	assert.Equal(t, "test_k1", entry.Fields[0].Name)
	assert.Equal(t, "test_v1", entry.Fields[0].Value)
	assert.Equal(t, "test k2", entry.Fields[1].Name)
	assert.Equal(t, "test v2", entry.Fields[1].Value)
	entry, err = parser.ParseNext()
	assert.NoError(t, err)
	assert.Equal(t, 2021, entry.Header.DateTime.Year())
	assert.Equal(t, 8, int(entry.Header.DateTime.Month()))
	assert.Equal(t, 4, entry.Header.DateTime.Day())
	assert.Equal(t, 12, entry.Header.DateTime.Hour())
	assert.Equal(t, 0, entry.Header.DateTime.Minute())
	assert.Equal(t, 43, entry.Header.DateTime.Second())
	assert.Equal(t, 129*1000*1000, entry.Header.DateTime.Nanosecond())
	assert.Equal(t, LogLevelInfo, entry.Header.Level)
	assert.Equal(t, "lib.rs", entry.Header.File)
	assert.Equal(t, 86, entry.Header.Line)
	assert.Equal(t, "Release Version:   5.1.0-alpha", entry.Message)
	assert.Len(t, entry.Fields, 0)
}

func TestStreamParser_ParseNext(t *testing.T) {
	testStreamParserParseNext(t, `[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]
[2021/08/04 12:00:43.129 +08:00] [DEBUG] [<unknown>] [test_message] [test_k1=test_v1] ["test k2"="test v2"]
[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]`)
	testStreamParserParseNext(t, `
[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]
[2021/08/04 12:00:43.129 +08:00] [DEBUG] [<unknown>] [test_message] [test_k1=test_v1] ["test k2"="test v2"]
[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]`)
	testStreamParserParseNext(t, `[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]
[2021/08/04 12:00:43.129 +08:00] [DEBUG] [<unknown>] [test_message] [test_k1=test_v1] ["test k2"="test v2"]
[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]
`)
	testStreamParserParseNext(t, `

[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]

[2021/08/04 12:00:43.129 +08:00] [DEBUG] [<unknown>] [test_message] [test_k1=test_v1] ["test k2"="test v2"]

[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]

`)
}

func TestParseFromString(t *testing.T) {
	entries, err := ParseFromString(`[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]
[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]`)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestParseFromBytes(t *testing.T) {
	entries, err := ParseFromBytes([]byte(`[2021/08/04 12:00:43.128 +08:00] [INFO] [lib.rs:81] ["Welcome to TiKV"]
[2021/08/04 12:00:43.129 +08:00] [INFO] [lib.rs:86] ["Release Version:   5.1.0-alpha"]`))
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
}
