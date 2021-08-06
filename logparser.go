// Package logparser provides a parser implementation for parsing Unified Log Format.
//
// For more information about the Unified Log Format,
// see https://github.com/tikv/rfcs/blob/master/text/0018-unified-log-format.md
package logparser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// LogLevel is an enumeration type for the log level.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota - 1
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	case LogLevelFatal:
		return "FATAL"
	default:
		return fmt.Sprintf("LEVEL(%d)", l) // unreachable
	}
}

// StringToLogLevel converts the string log level to the enumeration type.
// An error is returned if the string is not recognized.
func StringToLogLevel(s string) (LogLevel, error) {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return LogLevelDebug, nil
	case "INFO":
		return LogLevelInfo, nil
	case "WARN":
		return LogLevelWarn, nil
	case "ERROR":
		return LogLevelError, nil
	case "FATAL":
		return LogLevelFatal, nil
	default:
		return LogLevelInfo, fmt.Errorf("unexpected log level string '%s'", s)
	}
}

// LogHeader defines the header of one log.
type LogHeader struct {
	DateTime time.Time
	Level    LogLevel
	File     string
	Line     int
}

// LogField defines one k/v field of one log.
type LogField struct {
	Name  string
	Value string
}

// LogEntry defines an entire log entry.
type LogEntry struct {
	Header  LogHeader
	Message string
	Fields  []LogField // TODO: considering hashmap
}

// ParseFromBytes parses a byte slice as *LogEntry slice.
func ParseFromBytes(r []byte) ([]*LogEntry, error) {
	return ParseFromReader(bytes.NewReader(r))
}

// ParseFromString parses a string as *LogEntry slice.
func ParseFromString(r string) ([]*LogEntry, error) {
	return ParseFromReader(strings.NewReader(r))
}

// ParseFromReader parses a byte stream from io.Reader as *LogEntry slice.
// The function continues to run until the reader returns io.EOF.
func ParseFromReader(r io.Reader) ([]*LogEntry, error) {
	var entries []*LogEntry
	p := NewStreamParser(r)
	for {
		entry, err := p.ParseNext()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// StreamParser is a parser implementation which parses bytes from
// io.Reader into individual *LogEntry. Users can parse large log files
// on demand without having to read them all into memory at once.
type StreamParser struct {
	br          *bufio.Reader
	line        int
	datetimeBuf [30]byte
	levelBuf    [5]byte
}

// NewStreamParser creates new *StreamParser associated with the io.Reader.
func NewStreamParser(r io.Reader) *StreamParser {
	return &StreamParser{
		br:   bufio.NewReader(r),
		line: 1,
	}
}

// ParseNext reads and parses one LogEntry from bufio.Reader on demand.
// This function will return (nil, nil) if the underlying io.Reader returns
// io.EOF in the standard case.
func (p *StreamParser) ParseNext() (*LogEntry, error) {
	// Skip empty lines.
	if err := p.trimNewLines(); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, p.wrapErr(err)
	}
	// Skip spaces at the beginning of the line.
	if err := p.trimChar(' '); err != nil {
		return nil, p.wrapErr(err)
	}
	// Parse datetime.
	datetime, err := p.parseDatetime()
	if err != nil {
		return nil, p.wrapErr(err)
	}
	// Skip one space.
	if err := p.skipChar(' '); err != nil {
		return nil, p.wrapErr(err)
	}
	// Parse log level.
	level, err := p.parseLogLevel()
	if err != nil {
		return nil, p.wrapErr(err)
	}
	// Skip one space.
	if err := p.skipChar(' '); err != nil {
		return nil, p.wrapErr(err)
	}
	// Parse file:line.
	filename, line, err := p.parseFileLine()
	if err != nil {
		return nil, p.wrapErr(err)
	}
	// Skip one space.
	if err := p.skipChar(' '); err != nil {
		return nil, p.wrapErr(err)
	}
	// Parse message.
	message, err := p.parseMessage()
	if err != nil {
		return nil, p.wrapErr(err)
	}
	// Parse fields.
	fields, err := p.parseFields()
	if err != nil {
		return nil, p.wrapErr(err)
	}
	// Skip spaces at the end of the line.
	if err := p.trimChar(' '); err != nil && err != io.EOF {
		return nil, p.wrapErr(err)
	}
	return &LogEntry{
		Header: LogHeader{
			DateTime: datetime,
			Level:    level,
			File:     filename,
			Line:     line,
		},
		Message: message,
		Fields:  fields,
	}, nil
}

func (p *StreamParser) wrapErr(cause error) error {
	return fmt.Errorf("invalid log format at line %d, cause: %v", p.line, cause)
}

func (p *StreamParser) skipChar(expect rune) error {
	c, _, err := p.br.ReadRune()
	if err != nil {
		return err
	}
	if c != expect {
		return fmt.Errorf("expect '%c' but found '%c'", expect, c)
	}
	return nil
}

func (p *StreamParser) trimChar(skip rune) error {
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return err
		}
		if c != skip {
			return p.br.UnreadRune()
		}
	}
}

func (p *StreamParser) trimNewLines() error {
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return err
		}
		if c == '\r' {
			c, _, err = p.br.ReadRune()
			if err != nil {
				return err
			}
			if c != '\n' {
				return fmt.Errorf("expect '\\n' but found '%c'", c)
			}
		}
		if c != '\n' {
			return p.br.UnreadRune()
		}
		p.line++
	}
}

func (p *StreamParser) parseDatetime() (time.Time, error) {
	if err := p.skipChar('['); err != nil {
		return time.Time{}, err
	}
	n := 0
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return time.Time{}, err
		}
		if c == ']' {
			break
		}
		if !validDatetimeChar(c) {
			return time.Time{}, fmt.Errorf("unexpected character '%c'", c)
		}
		if n >= len(p.datetimeBuf) {
			return time.Time{}, errors.New("datetime too long")
		}
		p.datetimeBuf[n] = byte(c)
		n++
	}
	return time.Parse("2006/01/02 15:04:05.000 -07:00", string(p.datetimeBuf[:n]))
}

func (p *StreamParser) parseLogLevel() (LogLevel, error) {
	if err := p.skipChar('['); err != nil {
		return -1, err
	}
	n := 0
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return -1, err
		}
		if c == ']' {
			break
		}
		if !validLogLevelChar(c) {
			return -1, fmt.Errorf("unexpected character '%c'", c)
		}
		if n >= len(p.levelBuf) {
			return -1, errors.New("log level too long")
		}
		p.levelBuf[n] = byte(c)
		n++
	}
	return StringToLogLevel(string(p.levelBuf[:n]))
}

func (p *StreamParser) parseFileLine() (string, int, error) {
	if err := p.skipChar('['); err != nil {
		return "", 0, err
	}
	c, _, err := p.br.ReadRune()
	if err != nil {
		return "", 0, err
	}
	if c == '<' {
		// [<unknown>]
		for {
			c, _, err := p.br.ReadRune()
			if err != nil {
				return "", 0, err
			}
			if c == ']' {
				break
			}
			if !((c >= 'a' && c <= 'z') || c == '<' || c == '>') {
				return "", 0, fmt.Errorf("unexpected character '%c'", c)
			}
		}
		return "", 0, nil
	} else {
		if err := p.br.UnreadRune(); err != nil {
			return "", 0, err
		}
	}
	// [file:line]
	var filename, line []rune
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return "", 0, err
		}
		if c == ':' {
			break
		}
		if !validFilenameChar(c) {
			return "", 0, fmt.Errorf("unexpected character '%c'", c)
		}
		filename = append(filename, c)
	}
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return "", 0, err
		}
		if c == ']' {
			break
		}
		if !validLineNumberChar(c) {
			return "", 0, fmt.Errorf("unexpected character '%c'", c)
		}
		line = append(line, c)
	}
	lineNum, err := strconv.Atoi(string(line))
	if err != nil {
		panic(err) // unreachable
	}
	return string(filename), lineNum, nil
}

func (p *StreamParser) parseMessage() (string, error) {
	if err := p.skipChar('['); err != nil {
		return "", err
	}
	r, err := p.parseStringLiteral()
	if err != nil {
		return "", err
	}
	if err := p.skipChar(']'); err != nil {
		return "", err
	}
	return r, nil
}

func (p *StreamParser) parseFields() ([]LogField, error) {
	var fields []LogField
	for {
		if err := p.trimChar(' '); err != nil {
			if err == io.EOF {
				return fields, nil
			}
			return nil, err
		}
		c, _, err := p.br.ReadRune()
		if err != nil {
			return nil, err
		}
		if c != '[' {
			if err := p.br.UnreadRune(); err != nil {
				return nil, err
			}
			return fields, nil
		}
		name, err := p.parseStringLiteral()
		if err != nil {
			return nil, err
		}
		if err := p.skipChar('='); err != nil {
			return nil, err
		}
		value, err := p.parseStringLiteral()
		if err != nil {
			return nil, err
		}
		if err := p.skipChar(']'); err != nil {
			return nil, err
		}
		fields = append(fields, LogField{
			Name:  name,
			Value: value,
		})
	}
}

// TODO: optimize
func (p *StreamParser) parseStringLiteral() (string, error) {
	c, _, err := p.br.ReadRune()
	if err != nil {
		return "", err
	}
	if err := p.br.UnreadRune(); err != nil {
		return "", err
	}
	if c == '"' {
		return p.parseStringJson()
	}
	var literal []rune
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return "", err
		}
		if !validStringLiteralChar(c) {
			if err := p.br.UnreadRune(); err != nil {
				return "", err
			}
			break
		}
		literal = append(literal, c)
	}
	return string(literal), nil
}

// TODO: optimize
func (p *StreamParser) parseStringJson() (string, error) {
	quotes := 0
	var literal []rune
Loop:
	for {
		c, _, err := p.br.ReadRune()
		if err != nil {
			return "", err
		}
		literal = append(literal, c)
		switch c {
		case '\\':
			c, _, err := p.br.ReadRune()
			if err != nil {
				return "", err
			}
			literal = append(literal, c)
		case '"':
			quotes++
			if quotes == 2 {
				break Loop
			}
		}
	}
	var r string
	err := json.Unmarshal([]byte(string(literal)), &r)
	return r, err
}

func validDatetimeChar(c rune) bool {
	return (c >= '0' && c <= '9') ||
		c == '/' ||
		c == ' ' ||
		c == ':' ||
		c == '.' ||
		c == '+' ||
		c == '-'
}

func validLogLevelChar(c rune) bool {
	return c >= 'A' && c <= 'Z'
}

func validFilenameChar(c rune) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '.' ||
		c == '-' ||
		c == '_'
}

func validLineNumberChar(c rune) bool {
	return c >= '0' && c <= '9'
}

func validStringLiteralChar(c rune) bool {
	return !((c >= 0x0000 && c <= 0x0020) || c == '"' || c == '=' || c == '[' || c == ']')
}
