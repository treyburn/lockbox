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
	synctest.Test(t, func(t *testing.T) {
		mock := newMockReadWriteCloser("")
		logger := NewFileTransactionLogger(mock)

		assert.Nil(t, logger.events, "Expected events channel to be nil before Run()")
		assert.Nil(t, logger.errors, "Expected errors channel to be nil before Run()")

		logger.Run()

		// Give goroutine time to start
		time.Sleep(10 * time.Millisecond)

		assert.NotNil(t, logger.events, "Expected events channel to be initialized after Run()")
		assert.NotNil(t, logger.errors, "Expected errors channel to be initialized after Run()")

		err := logger.Close()
		assert.NoError(t, err)
		synctest.Wait()
	})
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
	synctest.Test(t, func(t *testing.T) {
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

		err := logger.Close()
		assert.NoError(t, err)
		synctest.Wait()
	})
}

// TestFileTransactionLogger_MixedOperations tests mixed PUT and DELETE operations
func TestFileTransactionLogger_MixedOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		err := logger.Close()
		assert.NoError(t, err)
		synctest.Wait()
	})
}

// TestFileTransactionLogger_Err tests the Err() method
func TestFileTransactionLogger_Err(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		err := logger.Close()
		assert.NoError(t, err)
		synctest.Wait()
	})
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
	synctest.Test(t, func(t *testing.T) {
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

		err := logger.Close()
		assert.NoError(t, err)
		synctest.Wait()
	})
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
			name: "empty values for delete",
			data: "1\t1\tkey1\t\n",
			expected: []Event{
				{Sequence: 1, Kind: EventDelete, Key: "key1", Value: ""},
			},
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

// TestFileTransactionLogger_WriteReadRoundTrip tests that events written via Run can be read back
// by ReadEvents. This is the highest-value test for the file logger since it validates the
// contract between the write and read halves.
//
// BUG: This test is expected to fail. WriteDelete produces "seq\t1\tkey\t\n" (empty value),
// but ReadEvents uses fmt.Sscanf with %s which cannot match an empty string. The serialization
// format does not round-trip for DELETE events.
func TestFileTransactionLogger_WriteReadRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := newMockReadWriteCloser("")
		logger := NewFileTransactionLogger(mock)
		logger.Run()

		time.Sleep(time.Millisecond)

		logger.WritePut("key1", "value1")
		logger.WriteDelete("key2")
		logger.WritePut("key3", "value3")

		time.Sleep(time.Millisecond)

		err := logger.Close()
		require.NoError(t, err)
		synctest.Wait()

		// Read back from the same buffer using a fresh logger (resets lastSequence)
		readLogger := NewFileTransactionLogger(mock)
		eventChan, errChan := readLogger.ReadEvents()

		var events []Event
		for e := range eventChan {
			events = append(events, e)
		}

		// Check for errors
		select {
		case err, ok := <-errChan:
			if ok {
				assert.NoError(t, err)
			}
		default:
		}

		require.Len(t, events, 3, "Expected all 3 written events to be read back")

		assert.Equal(t, EventPut, events[0].Kind)
		assert.Equal(t, "key1", events[0].Key)
		assert.Equal(t, "value1", events[0].Value)

		assert.Equal(t, EventDelete, events[1].Kind)
		assert.Equal(t, "key2", events[1].Key)
		assert.Equal(t, "", events[1].Value, "DELETE event should have empty value")

		assert.Equal(t, EventPut, events[2].Kind)
		assert.Equal(t, "key3", events[2].Key)
		assert.Equal(t, "value3", events[2].Value)
	})
}

// TestFileTransactionLogger_ReadEvents_DeleteHasEmptyValue verifies that ReadEvents correctly
// handles DELETE events that have an empty value field in the serialized format.
//
// BUG: This test is expected to fail. When a DELETE is written, the value field is empty,
// producing a line like "2\t1\tkey2\t\n". fmt.Sscanf cannot parse this because %s requires
// at least one non-whitespace character. ReadEvents will return a parse error on the DELETE
// line instead of successfully reading 2 events.
func TestFileTransactionLogger_ReadEvents_DeleteHasEmptyValue(t *testing.T) {
	// Simulate the exact output that Run() would produce for PUT("key1","value1") then DELETE("key2")
	data := "1\t2\tkey1\tvalue1\n2\t1\tkey2\t\n"
	mock := newMockReadWriteCloser(data)
	logger := NewFileTransactionLogger(mock)

	eventChan, errChan := logger.ReadEvents()

	var events []Event
	for e := range eventChan {
		events = append(events, e)
	}

	// Check for errors - we expect none for valid data
	select {
	case err, ok := <-errChan:
		if ok {
			assert.NoError(t, err, "ReadEvents should handle DELETE events with empty values")
		}
	default:
	}

	require.Len(t, events, 2, "Expected both PUT and DELETE events to be read")
	assert.Equal(t, "", events[1].Value, "DELETE event should have empty value, not a leaked value from the previous PUT")
}
