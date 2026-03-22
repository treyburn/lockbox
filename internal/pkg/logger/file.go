package logger

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
)

// compile time assertion that FileTransactionLogger is a TransactionLog
var _ TransactionLog = (*FileTransactionLogger)(nil)

func NewFileTransactionLogger(fileHandle io.ReadWriteCloser) *FileTransactionLogger {
	return &FileTransactionLogger{file: fileHandle}
}

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence atomic.Uint64
	file         io.ReadWriteCloser
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{Kind: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{Kind: EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events
	errs := make(chan error, 1)
	l.errors = errs
	go func() {
		for e := range events {
			seq := l.lastSequence.Add(1)

			_, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t%s\n", seq, e.Kind, e.Key, e.Value)
			if err != nil {
				errs <- fmt.Errorf("failed to process event: [%d-%d-%s]: %w", seq, e.Kind, e.Key, err)
				return
			}
		}
	}()
}

func parseEvent(line string) (Event, error) {
	fields := strings.Split(line, "\t")
	if len(fields) < 3 {
		return Event{}, fmt.Errorf("error parsing event: expected at least 3 tab-separated fields, got %d", len(fields))
	}

	seq, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return Event{}, fmt.Errorf("error parsing event: invalid sequence %q: %w", fields[0], err)
	}

	kind, err := strconv.ParseUint(fields[1], 10, 8)
	if err != nil {
		return Event{}, fmt.Errorf("error parsing event: invalid event kind %q: %w", fields[1], err)
	}

	e := Event{
		Sequence: seq,
		Kind:     EventKind(kind),
		Key:      fields[2],
	}

	if len(fields) >= 4 {
		e.Value = fields[3]
	}

	return e, nil
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outErr := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outErr)

		for scanner.Scan() {
			e, err := parseEvent(scanner.Text())
			if err != nil {
				outErr <- err
				return
			}

			// atomically compare and swap the value for our latest sequence
			for {
				last := l.lastSequence.Load()
				if last >= e.Sequence {
					outErr <- fmt.Errorf("transaction sequence out of sequence: %d >= %d", last, e.Sequence)
					return
				}

				if l.lastSequence.CompareAndSwap(last, e.Sequence) {
					outEvent <- e
					break
				}
			}
		}

		if err := scanner.Err(); err != nil {
			outErr <- fmt.Errorf("error reading events: %w", err)
			return
		}
	}()

	return outEvent, outErr
}

func (l *FileTransactionLogger) Close() error {
	close(l.events)
	return l.file.Close()
}

type Event struct {
	Sequence uint64
	Kind     EventKind
	Key      string
	Value    string
}

type EventKind byte

const (
	_ EventKind = iota
	EventDelete
	EventPut
)
