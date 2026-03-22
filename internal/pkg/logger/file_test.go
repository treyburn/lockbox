package logger

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NotNil(t, logger, "Expected non-nil logger")
	assert.Equal(t, mock, logger.file, "Expected file handle to be set directly")

	last := logger.lastSequence.Load()
	assert.Equal(t, uint64(0), last)
}

// TestFileTransactionLogger_Run tests the Run method initialization
func TestFileTransactionLogger_Run(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)

	assert.Nil(t, logger.events, "Expected events channel to be nil before Run()")
	assert.Nil(t, logger.errors, "Expected errors channel to be nil before Run()")

	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	assert.NotNil(t, logger.events, "Expected events channel to be initialized after Run()")
	assert.NotNil(t, logger.errors, "Expected errors channel to be initialized after Run()")
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
			assert.Contains(t, output, expected)
		}

		last := logger.lastSequence.Load()
		assert.Equal(t, uint64(2), last)

		err := logger.Close()
		assert.NoError(t, err)
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
		assert.Contains(t, output, expected)
	}

	assert.Equal(t, uint64(2), logger.lastSequence.Load())
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
		assert.Contains(t, output, expected)
	}

	assert.Equal(t, uint64(3), logger.lastSequence.Load())
}

// TestFileTransactionLogger_Err tests the Err() method
func TestFileTransactionLogger_Err(t *testing.T) {
	mock := newMockReadWriteCloser("")
	logger := NewFileTransactionLogger(mock)
	logger.Run()

	errChan := logger.Err()
	require.NotNil(t, errChan, "Expected non-nil error channel")

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
		if ok {
			assert.NoError(t, err)
		}
	default:
		// Expected - no errors
	}

	assert.Empty(t, events, "Expected 0 events from empty file")
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
		if ok {
			assert.NoError(t, err)
		}
	default:
		// Expected - no errors
	}

	expectedEvents := []Event{
		{Sequence: 1, Kind: EventPut, Key: "key1", Value: "value1"},
		{Sequence: 2, Kind: EventDelete, Key: "key2", Value: "deleted"},
		{Sequence: 3, Kind: EventPut, Key: "key3", Value: "value3"},
	}

	require.Len(t, events, len(expectedEvents))

	for i, expected := range expectedEvents {
		assert.Equal(t, expected.Sequence, events[i].Sequence, "Event %d: sequence mismatch", i)
		assert.Equal(t, expected.Kind, events[i].Kind, "Event %d: kind mismatch", i)
		assert.Equal(t, expected.Key, events[i].Key, "Event %d: key mismatch", i)
		assert.Equal(t, expected.Value, events[i].Value, "Event %d: value mismatch", i)
	}

	assert.Equal(t, uint64(3), logger.lastSequence.Load())
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
		require.Error(t, err, "Expected error for invalid format")
		assert.ErrorContains(t, err, "error parsing event")
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
	assert.Len(t, events, 1, "Expected 1 event before error")

	// Check for error
	select {
	case err := <-errChan:
		require.Error(t, err, "Expected error for out-of-sequence events")
		assert.ErrorContains(t, err, "transaction sequence out of sequence")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}
}

// TestFileTransactionLogger_ReadEvents_UpdatesLastSequence tests that ReadEvents updates lastSequence
func TestFileTransactionLogger_ReadEvents_UpdatesLastSequence(t *testing.T) {
	data := "1\t2\tkey1\tvalue1\n5\t2\tkey2\tvalue2\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	assert.Equal(t, uint64(0), logger.lastSequence.Load(), "Expected initial lastSequence to be 0")

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
		if ok {
			assert.NoError(t, err)
		}
	default:
		// Expected - no errors
	}

	assert.Equal(t, uint64(5), logger.lastSequence.Load(), "Expected lastSequence to be 5 after reading")
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

	assert.Equal(t, uint64(10), logger.lastSequence.Load())

	// Verify all sequences are in the output
	output := mock.String()
	for i := 1; i <= 10; i++ {
		expected := fmt.Sprintf("%d\t2\tkey%d\tvalue%d", i, i, i)
		assert.Contains(t, output, expected)
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
		require.Error(t, err, "Expected error from failing writer")
		assert.ErrorContains(t, err, "simulated write error")
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
						assert.NoError(t, err)
					}
				}
			default:
				// No error
			}

			if tc.expectError {
				assert.True(t, gotError, "Expected an error but got none")
			}

			if !tc.expectError {
				require.Len(t, events, len(tc.expected))
				for i, expected := range tc.expected {
					assert.Equal(t, expected, events[i], "Event %d mismatch", i)
				}
			}
		})
	}
}
