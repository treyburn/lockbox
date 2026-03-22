package logger

import (
	"database/sql"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewPostgresTransactionLogger tests the constructor
func TestNewPostgresTransactionLogger(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect ping
	mock.ExpectPing()

	// Expect table existence check - table exists
	rows := sqlmock.NewRows([]string{"to_regclass"}).AddRow("transactions")
	mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

	// Verify table exists returns true
	exists, err := logger.verifyTableExists()
	require.NoError(t, err)
	assert.True(t, exists)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestNewPostgresTransactionLogger_TableNotExists tests constructor when table doesn't exist
func TestNewPostgresTransactionLogger_TableNotExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect table existence check - return NULL (table doesn't exist)
	rows := sqlmock.NewRows([]string{"to_regclass"}).AddRow(nil)
	mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

	// Verify table doesn't exist (NULL result from to_regclass)
	exists, err := logger.verifyTableExists()
	require.NoError(t, err)
	assert.False(t, exists)

	// Expect table creation
	mock.ExpectExec(`CREATE TABLE transactions`).WillReturnResult(sqlmock.NewResult(0, 0))

	// Create table
	err = logger.createTable()
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestNewPostgresTransactionLogger_Constructor tests the full NewPostgresTransactionLogger constructor flow.
// The constructor does sql.Open -> Ping -> verifyTableExists -> conditionally createTable.
// This requires a real database connection and cannot be tested with sqlmock since sql.Open
// is called internally with a connection string.
func TestNewPostgresTransactionLogger_Constructor(t *testing.T) {
	t.Skip("TODO: integration test - requires real Postgres connection to test full constructor flow (sql.Open -> Ping -> verifyTableExists -> createTable)")
}

// TestPostgresTransactionLogger_Run tests the Run method initialization
func TestPostgresTransactionLogger_Run(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	logger := &PostgresTransactionLogger{db: db}

	assert.Nil(t, logger.events, "Expected events channel to be nil before Run()")
	assert.Nil(t, logger.errors, "Expected errors channel to be nil before Run()")

	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	assert.NotNil(t, logger.events, "Expected events channel to be initialized after Run()")
	assert.NotNil(t, logger.errors, "Expected errors channel to be initialized after Run()")
}

// TestPostgresTransactionLogger_WritePut tests writing PUT events
func TestPostgresTransactionLogger_WritePut(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect two INSERT queries
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key1", "value1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key2", "value2").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WritePut("key1", "value1")
	logger.WritePut("key2", "value2")

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	err = logger.Close()
	assert.NoError(t, err)

	// Give time for goroutine to process remaining events
	time.Sleep(10 * time.Millisecond)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_WriteDelete tests writing DELETE events
func TestPostgresTransactionLogger_WriteDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect two INSERT queries for delete events
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventDelete, "key1", "").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventDelete, "key2", "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WriteDelete("key1")
	logger.WriteDelete("key2")

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	err = logger.Close()
	assert.NoError(t, err)

	// Give time for goroutine to process remaining events
	time.Sleep(10 * time.Millisecond)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_MixedOperations tests mixed PUT and DELETE operations
func TestPostgresTransactionLogger_MixedOperations(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect mixed INSERT queries
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key1", "value1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventDelete, "key2", "").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key3", "value3").
		WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WritePut("key1", "value1")
	logger.WriteDelete("key2")
	logger.WritePut("key3", "value3")

	// Give time for writes to complete
	time.Sleep(50 * time.Millisecond)

	err = logger.Close()
	assert.NoError(t, err)

	// Give time for goroutine to process remaining events
	time.Sleep(10 * time.Millisecond)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_Err tests the Err() method
func TestPostgresTransactionLogger_Err(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	logger := &PostgresTransactionLogger{db: db}
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

// TestPostgresTransactionLogger_ReadEvents_EmptyTable tests reading from an empty table
func TestPostgresTransactionLogger_ReadEvents_EmptyTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return empty rows
	rows := sqlmock.NewRows([]string{"sequence", "event_type", "key", "value"})
	mock.ExpectQuery(`SELECT sequence, event_type, key, value FROM transactions`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

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

	// Check for errors
	select {
	case err, ok := <-errChan:
		if ok {
			assert.NoError(t, err)
		}
	default:
		// Expected - no errors
	}

	assert.Empty(t, events, "Expected 0 events from empty table")

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_ReadEvents_ValidData tests reading valid events
func TestPostgresTransactionLogger_ReadEvents_ValidData(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return valid rows
	rows := sqlmock.NewRows([]string{"sequence", "event_type", "key", "value"}).
		AddRow(1, EventPut, "key1", "value1").
		AddRow(2, EventDelete, "key2", "").
		AddRow(3, EventPut, "key3", "value3")
	mock.ExpectQuery(`SELECT sequence, event_type, key, value FROM transactions`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

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

	// Check for errors
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
		{Sequence: 2, Kind: EventDelete, Key: "key2", Value: ""},
		{Sequence: 3, Kind: EventPut, Key: "key3", Value: "value3"},
	}

	require.Len(t, events, len(expectedEvents))

	for i, expected := range expectedEvents {
		assert.Equal(t, expected.Sequence, events[i].Sequence, "Event %d: sequence mismatch", i)
		assert.Equal(t, expected.Kind, events[i].Kind, "Event %d: kind mismatch", i)
		assert.Equal(t, expected.Key, events[i].Key, "Event %d: key mismatch", i)
		assert.Equal(t, expected.Value, events[i].Value, "Event %d: value mismatch", i)
	}

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_ReadEvents_QueryError tests error handling when query fails
func TestPostgresTransactionLogger_ReadEvents_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return query error
	mock.ExpectQuery(`SELECT sequence, event_type, key, value FROM transactions`).
		WillReturnError(fmt.Errorf("database connection lost"))

	logger := &PostgresTransactionLogger{db: db}

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
		require.Error(t, err, "Expected error for query failure")
		assert.ErrorContains(t, err, "failed to read transactions")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_ReadEvents_ScanError tests error handling when row scan fails
func TestPostgresTransactionLogger_ReadEvents_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return rows with invalid data type for sequence (string instead of int)
	rows := sqlmock.NewRows([]string{"sequence", "event_type", "key", "value"}).
		AddRow("invalid", EventPut, "key1", "value1")
	mock.ExpectQuery(`SELECT sequence, event_type, key, value FROM transactions`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

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
		require.Error(t, err, "Expected error for scan failure")
		assert.ErrorContains(t, err, "failed to read row")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_ReadEvents_ScanError_NoCorruptEvents verifies that when a row
// scan fails, no events are sent on the event channel.
//
// BUG: This test is expected to fail. In postgres.go ReadEvents, when rows.Scan fails, the
// error is sent on outErr but execution falls through to "outEvent <- e" instead of continuing
// or returning. This sends a corrupt/zero-value event to the consumer.
func TestPostgresTransactionLogger_ReadEvents_ScanError_NoCorruptEvents(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return rows with invalid data type for sequence (string instead of int)
	rows := sqlmock.NewRows([]string{"sequence", "event_type", "key", "value"}).
		AddRow("invalid", EventPut, "key1", "value1")
	mock.ExpectQuery(`SELECT sequence, event_type, key, value FROM transactions`).WillReturnRows(rows)

	logger := &PostgresTransactionLogger{db: db}

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

	// Drain error channel
	select {
	case <-errChan:
	default:
	}

	// No events should have been sent when scan failed
	assert.Empty(t, events, "Expected no events when scan fails, but got a corrupt event")

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_WriteError tests error handling during writes
func TestPostgresTransactionLogger_WriteError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Return error on INSERT
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key1", "value1").
		WillReturnError(fmt.Errorf("simulated write error"))

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	logger.WritePut("key1", "value1")

	// Wait for error to be sent
	select {
	case err := <-logger.Err():
		require.Error(t, err, "Expected error from failing write")
		assert.ErrorContains(t, err, "simulated write error")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_SequenceIncrement tests that multiple writes work correctly
func TestPostgresTransactionLogger_SequenceIncrement(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Expect 10 INSERT queries
	for i := 1; i <= 10; i++ {
		mock.ExpectExec(`INSERT INTO transactions`).
			WithArgs(EventPut, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)).
			WillReturnResult(sqlmock.NewResult(int64(i), 1))
	}
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Write multiple events
	for i := 1; i <= 10; i++ {
		logger.WritePut(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Give time for writes to complete
	time.Sleep(100 * time.Millisecond)

	err = logger.Close()
	assert.NoError(t, err)

	// Give time for goroutine to process remaining events
	time.Sleep(10 * time.Millisecond)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_verifyTableExists tests table existence check
func TestPostgresTransactionLogger_verifyTableExists(t *testing.T) {
	t.Run("table exists", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer dbCleanup(t, db, mock)

		rows := sqlmock.NewRows([]string{"to_regclass"}).AddRow("transactions")
		mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(rows)

		logger := &PostgresTransactionLogger{db: db}

		exists, err := logger.verifyTableExists()
		require.NoError(t, err)
		assert.True(t, exists)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("table does not exist - empty result", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer dbCleanup(t, db, mock)

		// Return no rows - simulates case where table doesn't exist
		rows := sqlmock.NewRows([]string{"to_regclass"})
		mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(rows)

		logger := &PostgresTransactionLogger{db: db}

		exists, err := logger.verifyTableExists()
		require.NoError(t, err)
		assert.False(t, exists)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})

	t.Run("table does not exist - NULL result", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer dbCleanup(t, db, mock)

		// Return NULL - this is what PostgreSQL's to_regclass returns when table doesn't exist
		rows := sqlmock.NewRows([]string{"to_regclass"}).AddRow(nil)
		mock.ExpectQuery(`SELECT to_regclass`).WillReturnRows(rows)

		logger := &PostgresTransactionLogger{db: db}

		exists, err := logger.verifyTableExists()
		require.NoError(t, err)
		assert.False(t, exists)

		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}

// TestPostgresTransactionLogger_verifyTableExists_Error tests error handling in table check
func TestPostgresTransactionLogger_verifyTableExists_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	mock.ExpectQuery(`SELECT to_regclass`).WillReturnError(fmt.Errorf("database error"))

	logger := &PostgresTransactionLogger{db: db}

	_, err = logger.verifyTableExists()
	assert.Error(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_createTable tests table creation
func TestPostgresTransactionLogger_createTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	mock.ExpectExec(`CREATE TABLE transactions`).WillReturnResult(sqlmock.NewResult(0, 0))

	logger := &PostgresTransactionLogger{db: db}

	err = logger.createTable()
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_createTable_Error tests error handling in table creation
func TestPostgresTransactionLogger_createTable_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	mock.ExpectExec(`CREATE TABLE transactions`).WillReturnError(fmt.Errorf("table already exists"))

	logger := &PostgresTransactionLogger{db: db}

	err = logger.createTable()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "failed to create table")

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_Close tests the Close method
func TestPostgresTransactionLogger_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	// Set up expectations for write
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key1", "value1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Write an event
	logger.WritePut("key1", "value1")

	// Give time for write to complete
	time.Sleep(50 * time.Millisecond)

	// Close should not return an error
	err = logger.Close()
	assert.NoError(t, err)

	// Give time for goroutine to finish
	time.Sleep(10 * time.Millisecond)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestPostgresTransactionLogger_WriteReadRoundTrip is a stub for testing that events written
// via Run can be read back via ReadEvents and produce consistent results.
// This requires a real Postgres database to be meaningful since sqlmock-based tests
// just verify SQL strings rather than actual data persistence.
func TestPostgresTransactionLogger_WriteReadRoundTrip(t *testing.T) {
	t.Skip("TODO: integration test - requires real Postgres to verify write->read round-trip consistency")
}

// TestPostgresTransactionLogger_CloseDeadlockOnUnreadErrors verifies that Close() does not
// deadlock when the error channel is full and unread.
//
// BUG: This test is expected to fail (timeout). In postgres.go Run(), when db.Exec fails,
// the error is sent on errs (buffered at 1). If the consumer never reads from Err(), the
// second error send blocks the goroutine on "errs <- err". Since the goroutine is stuck
// and never re-enters the select, Close()'s "p.done <- struct{}{}" blocks forever.
func TestPostgresTransactionLogger_CloseDeadlockOnUnreadErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// Both writes will fail
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key1", "value1").
		WillReturnError(fmt.Errorf("error 1"))
	mock.ExpectExec(`INSERT INTO transactions`).
		WithArgs(EventPut, "key2", "value2").
		WillReturnError(fmt.Errorf("error 2"))
	mock.ExpectClose()

	logger := &PostgresTransactionLogger{db: db}
	logger.Run()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// First write error fills the error channel buffer (size 1)
	logger.WritePut("key1", "value1")
	time.Sleep(50 * time.Millisecond)

	// Second write error blocks the goroutine trying to send on full error channel
	logger.WritePut("key2", "value2")
	time.Sleep(50 * time.Millisecond)

	// Intentionally do NOT read from Err() - the error channel is full
	// The goroutine is stuck on "errs <- err" and will never receive from done

	// Close should complete without deadlocking
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()

	select {
	case err := <-closeDone:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close() deadlocked - goroutine is stuck sending on full error channel")
	}
}

// TestPostgresTransactionLogger_EventsNotDroppedOnClose verifies that all buffered events
// are processed before the Run goroutine exits on Close().
//
// BUG: This test is expected to fail. The postgres Run() goroutine uses select{} between
// the done and events channels. When Close() sends on done, Go's select is non-deterministic
// when multiple cases are ready. If done is selected before all events are drained, buffered
// events are silently lost. Compare with file logger's Close() which uses close(events),
// allowing "for range events" to drain naturally.
//
// synctest is used here for more deterministic goroutine scheduling: events are written to
// the buffered channel while the goroutine hasn't run yet, then Close() fires immediately.
func TestPostgresTransactionLogger_EventsNotDroppedOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		// Expect all 5 events to be written
		for i := 1; i <= 5; i++ {
			mock.ExpectExec(`INSERT INTO transactions`).
				WithArgs(EventPut, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)).
				WillReturnResult(sqlmock.NewResult(int64(i), 1))
		}
		mock.ExpectClose()

		logger := &PostgresTransactionLogger{db: db}
		logger.Run()

		// Let goroutine start and enter select
		time.Sleep(time.Millisecond)

		// Write events into the buffered channel
		for i := 1; i <= 5; i++ {
			logger.WritePut(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		// Close immediately without giving the goroutine time to process
		// The goroutine should drain all events before exiting
		err = logger.Close()
		assert.NoError(t, err)

		synctest.Wait()

		// If events were dropped, not all INSERT expectations will be met
		err = mock.ExpectationsWereMet()
		assert.NoError(t, err, "All buffered events should be processed before shutdown")
	})
}

func dbCleanup(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock) {
	mock.ExpectClose()
	err := db.Close()
	assert.NoError(t, err)
}
