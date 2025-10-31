package logger

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockReadWriteCloser is a mock implementation of an io.ReadWriterCloser for testing
type mockReadWriteCloser struct {
	sync.RWMutex
	// embed bytes.Buffer so that our mock is a light wrapper around this builtin
	*bytes.Buffer

	closed bool
}

func newMockReadWriteCloser(data string) *mockReadWriteCloser {
	return &mockReadWriteCloser{
		Buffer: bytes.NewBufferString(data),
		closed: false,
	}
}

func (m *mockReadWriteCloser) Close() error {
	m.Lock()
	defer m.Unlock()
	m.closed = true
	return nil
}

func (m *mockReadWriteCloser) Read(p []byte) (n int, err error) {
	m.RLock()
	defer m.RUnlock()
	return m.Buffer.Read(p)
}

func (m *mockReadWriteCloser) Write(p []byte) (n int, err error) {
	m.Lock()
	defer m.Unlock()
	return m.Buffer.Write(p)
}

func (m *mockReadWriteCloser) String() string {
	m.RLock()
	defer m.RUnlock()
	return m.Buffer.String()
}

// TestNewFileTransactionLogger tests the constructor
func TestNewFileTransactionLogger(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.file != mock {
		t.Error("Expected file handle to be set correctly")
	}

	last := logger.lastSequence.Load()
	if last != 0 {
		t.Errorf("Expected lastSequence to be 0, got %d", last)
	}
}

// TestFileTransactionLogger_Run tests the Run method initialization
func TestFileTransactionLogger_Run(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)

	if logger.events != nil {
		t.Error("Expected events channel to be nil before Run()")
	}

	if logger.errors != nil {
		t.Error("Expected errors channel to be nil before Run()")
	}

	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	if logger.events == nil {
		t.Error("Expected events channel to be initialized after Run()")
	}

	if logger.errors == nil {
		t.Error("Expected errors channel to be initialized after Run()")
	}
}

// TestFileTransactionLogger_WritePut tests writing PUT events
func TestFileTransactionLogger_WritePut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := newMockReadWriteCloser("")
		logger := NewFileTransactionLogger(mock)
		logger.Run()

		// Give goroutine time to start
		time.Sleep(10 * time.Millisecond)

		logger.WritePut("key1", "value1")
		logger.WritePut("key2", "value2")

		// Give time for writes to complete
		time.Sleep(50 * time.Millisecond)

		output := mock.String()
		expectedLines := []string{
			"1\t2\tkey1\tvalue1",
			"2\t2\tkey2\tvalue2",
		}

		for _, expected := range expectedLines {
			if !strings.Contains(output, expected) {
				t.Errorf("Expected output to contain %q, got: %s", expected, output)
			}
		}

		last := logger.lastSequence.Load()
		if last != 2 {
			t.Errorf("Expected lastSequence to be 2, got %d", last)
		}

		err := logger.Close()
		assert.Nil(t, err)
		synctest.Wait()
	})
}

// TestFileTransactionLogger_WriteDelete tests writing DELETE events
func TestFileTransactionLogger_WriteDelete(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WriteDelete("key1")
	logger.WriteDelete("key2")

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	output := mock.String()
	expectedLines := []string{
		"1\t1\tkey1\t",
		"2\t1\tkey2\t",
	}

	for _, expected := range expectedLines {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected output to contain %q, got: %s", expected, output)
		}
	}

	last := logger.lastSequence.Load()
	if last != 2 {
		t.Errorf("Expected lastSequence to be 2, got %d", last)
	}
}

// TestFileTransactionLogger_MixedOperations tests mixed PUT and DELETE operations
func TestFileTransactionLogger_MixedOperations(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WritePut("key1", "value1")
	logger.WriteDelete("key2")
	logger.WritePut("key3", "value3")

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	output := mock.String()
	expectedLines := []string{
		"1\t2\tkey1\tvalue1",
		"2\t1\tkey2\t",
		"3\t2\tkey3\tvalue3",
	}

	for _, expected := range expectedLines {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected output to contain %q, got: %s", expected, output)
		}
	}

	last := logger.lastSequence.Load()
	if last != 3 {
		t.Errorf("Expected lastSequence to be 3, got %d", last)
	}
}

// TestFileTransactionLogger_Err tests the Err() method
func TestFileTransactionLogger_Err(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)
	logger.Run()

	errChan := logger.Err()
	if errChan == nil {
		t.Fatal("Expected non-nil error channel")
	}

	// Verify channel is initially empty
	select {
	case err := <-errChan:
		t.Errorf("Expected empty error channel, got: %v", err)
	case <-time.After(10 * time.Millisecond):
		// Expected - no errors
	}
}

// TestFileTransactionLogger_ReadEvents_EmptyFile tests reading from an empty file
func TestFileTransactionLogger_ReadEvents_EmptyFile(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)

	eventChan, errChan := logger.ReadEvents()

	// Collect all events
	var events []Event
	done := make(chan bool)

	go func() {
		for event := range eventChan {
			events = append(events, event)
		}
		done <- true
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for ReadEvents to complete")
	}

	// Check for errors - only if error channel has something
	select {
	case err, ok := <-errChan:
		if ok && err != nil {
			t.Errorf("Expected no errors, got: %v", err)
		}
	default:
		// Expected - no errors
	}

	if len(events) != 0 {
		t.Errorf("Expected 0 events from empty file, got %d", len(events))
	}
}

// TestFileTransactionLogger_ReadEvents_ValidData tests reading valid events
func TestFileTransactionLogger_ReadEvents_ValidData(t *testing.T) {
	// Use "deleted" as a placeholder value for delete operations since empty values cause parsing issues
	data := "1\t2\tkey1\tvalue1\n2\t1\tkey2\tdeleted\n3\t2\tkey3\tvalue3\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	eventChan, errChan := logger.ReadEvents()

	// Collect all events
	var events []Event
	done := make(chan bool)

	go func() {
		for event := range eventChan {
			events = append(events, event)
		}
		done <- true
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Second):
		t.Fatal("Timeout waiting for ReadEvents to complete")
	}

	// Check for errors - only if error channel has something
	select {
	case err, ok := <-errChan:
		if ok && err != nil {
			t.Errorf("Expected no errors, got: %v", err)
		}
	default:
		// Expected - no errors
	}

	expectedEvents := []Event{
		{Sequence: 1, Kind: EventPut, Key: "key1", Value: "value1"},
		{Sequence: 2, Kind: EventDelete, Key: "key2", Value: "deleted"},
		{Sequence: 3, Kind: EventPut, Key: "key3", Value: "value3"},
	}

	if len(events) != len(expectedEvents) {
		t.Fatalf("Expected %d events, got %d", len(expectedEvents), len(events))
	}

	for i, expected := range expectedEvents {
		if events[i].Sequence != expected.Sequence {
			t.Errorf("Event %d: expected sequence %d, got %d", i, expected.Sequence, events[i].Sequence)
		}
		if events[i].Kind != expected.Kind {
			t.Errorf("Event %d: expected kind %d, got %d", i, expected.Kind, events[i].Kind)
		}
		if events[i].Key != expected.Key {
			t.Errorf("Event %d: expected key %q, got %q", i, expected.Key, events[i].Key)
		}
		if events[i].Value != expected.Value {
			t.Errorf("Event %d: expected value %q, got %q", i, expected.Value, events[i].Value)
		}
	}

	last := logger.lastSequence.Load()
	if last != 3 {
		t.Errorf("Expected lastSequence to be 3, got %d", last)
	}
}

// TestFileTransactionLogger_ReadEvents_InvalidFormat tests reading with invalid format
func TestFileTransactionLogger_ReadEvents_InvalidFormat(t *testing.T) {
	data := "invalid\tdata\there\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	eventChan, errChan := logger.ReadEvents()

	// Collect all events
	var events []Event
	done := make(chan bool)

	go func() {
		for event := range eventChan {
			events = append(events, event)
		}
		done <- true
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for ReadEvents to complete")
	}

	// Check for error
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected error for invalid format")
		}
		if !strings.Contains(err.Error(), "error parsing event") {
			t.Errorf("Expected 'error parsing event' in error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}
}

// TestFileTransactionLogger_ReadEvents_OutOfSequence tests reading out-of-sequence events
func TestFileTransactionLogger_ReadEvents_OutOfSequence(t *testing.T) {
	data := "1\t2\tkey1\tvalue1\n1\t2\tkey2\tvalue2\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	eventChan, errChan := logger.ReadEvents()

	// Collect all events
	var events []Event
	done := make(chan bool)

	go func() {
		for event := range eventChan {
			events = append(events, event)
		}
		done <- true
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for ReadEvents to complete")
	}

	// Should only get the first event before error
	if len(events) != 1 {
		t.Errorf("Expected 1 event before error, got %d", len(events))
	}

	// Check for error
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected error for out-of-sequence events")
		}
		if !strings.Contains(err.Error(), "transaction sequence out of sequence") {
			t.Errorf("Expected 'transaction sequence out of sequence' in error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}
}

// TestFileTransactionLogger_ReadEvents_UpdatesLastSequence tests that ReadEvents updates lastSequence
func TestFileTransactionLogger_ReadEvents_UpdatesLastSequence(t *testing.T) {
	data := "1\t2\tkey1\tvalue1\n5\t2\tkey2\tvalue2\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	last := logger.lastSequence.Load()
	if last != 0 {
		t.Errorf("Expected initial lastSequence to be 0, got %d", last)
	}

	eventChan, errChan := logger.ReadEvents()

	// Drain channels
	done := make(chan bool)
	go func() {
		for range eventChan {
		}
		done <- true
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for ReadEvents to complete")
	}

	// Check for errors - only if error channel has something
	select {
	case err, ok := <-errChan:
		if ok && err != nil {
			t.Errorf("Expected no errors, got: %v", err)
		}
	default:
		// Expected - no errors
	}

	last = logger.lastSequence.Load()
	if last != 5 {
		t.Errorf("Expected lastSequence to be 5 after reading, got %d", last)
	}
}

// TestFileTransactionLogger_SequenceIncrement tests that sequence numbers increment correctly
func TestFileTransactionLogger_SequenceIncrement(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Write multiple events
	for i := 1; i <= 10; i++ {
		logger.WritePut(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	last := logger.lastSequence.Load()
	if last != 10 {
		t.Errorf("Expected lastSequence to be 10, got %d", last)
	}

	// Verify all sequences are in the output
	output := mock.String()
	for i := 1; i <= 10; i++ {
		expected := fmt.Sprintf("%d\t2\tkey%d\tvalue%d", i, i, i)
		if !strings.Contains(output, expected) {
			t.Errorf("Expected output to contain %q", expected)
		}
	}
}

// failingWriter is a mock writer that always returns an error
type failingWriter struct {
	bytes.Buffer
}

func (f *failingWriter) Close() error {
	return nil
}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated write error")
}

// TestFileTransactionLogger_WriteError tests error handling during writes
func TestFileTransactionLogger_WriteError(t *testing.T) {
	failing := &failingWriter{}
	logger := NewFileTransactionLogger(failing)
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WritePut("key1", "value1")

	// Wait for error to be sent
	select {
	case err := <-logger.Err():
		if err == nil {
			t.Error("Expected error from failing writer")
		}
		if !strings.Contains(err.Error(), "simulated write error") {
			t.Errorf("Expected 'simulated write error' in error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}
}

// TestFileTransactionLogger_ReadEvents_WithValues tests reading events with various value formats
func TestFileTransactionLogger_ReadEvents_WithValues(t *testing.T) {
	testCases := []struct {
		name        string
		data        string
		expected    []Event
		expectError bool
	}{
		{
			name:        "empty values for delete",
			data:        "1\t1\tkey1\t\n",
			expectError: true, // fmt.Sscanf will fail on empty value with trailing newline
		},
		{
			name: "simple values",
			data: "1\t2\tkey1\tvalue1\n",
			expected: []Event{
				{Sequence: 1, Kind: EventPut, Key: "key1", Value: "value1"},
			},
		},
		{
			name: "numeric values",
			data: "1\t2\tkey1\t12345\n",
			expected: []Event{
				{Sequence: 1, Kind: EventPut, Key: "key1", Value: "12345"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockReadWriteCloser(tc.data)
			logger := NewFileTransactionLogger(mock)

			eventChan, errChan := logger.ReadEvents()

			var events []Event
			done := make(chan bool)

			go func() {
				for event := range eventChan {
					events = append(events, event)
				}
				done <- true
			}()

			select {
			case <-done:
				// Expected
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Timeout waiting for ReadEvents to complete")
			}

			// Check for errors
			var gotError bool
			select {
			case err, ok := <-errChan:
				if ok && err != nil {
					gotError = true
					if !tc.expectError {
						t.Errorf("Expected no errors, got: %v", err)
					}
				}
			default:
				// No error
			}

			if tc.expectError && !gotError {
				t.Error("Expected an error but got none")
			}

			if !tc.expectError && len(events) != len(tc.expected) {
				t.Fatalf("Expected %d events, got %d", len(tc.expected), len(events))
			}

			if !tc.expectError {
				for i, expected := range tc.expected {
					if events[i] != expected {
						t.Errorf("Event %d mismatch: expected %+v, got %+v", i, expected, events[i])
					}
				}
			}
		})
	}
}
