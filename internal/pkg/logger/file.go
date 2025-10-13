package logger

import (
	"bufio"
	"fmt"
	"io"
)

// compile time assertion that FileTransactionLogger is a TransactionLog
var _ TransactionLog = (*FileTransactionLogger)(nil)

func NewTransactionLog(fileHandle io.ReadWriteCloser) TransactionLog {
	return &FileTransactionLogger{file: fileHandle}
}

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
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
			l.lastSequence++

			_, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t%s\n", l.lastSequence, e.Kind, e.Key, e.Value)
			if err != nil {
				errs <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outErr := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outErr)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.Kind, &e.Key, &e.Value); err != nil {
				outErr <- fmt.Errorf("error parsing event: %w", err)
				return
			}

			if l.lastSequence >= e.Sequence {
				outErr <- fmt.Errorf("transaction sequence out of sequence: %d >= %d", l.lastSequence, e.Sequence)
				return
			}

			l.lastSequence = e.Sequence
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outErr <- fmt.Errorf("error reading events: %w", err)
			return
		}
	}()

	return outEvent, outErr
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
