package logger

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewPostgresTransactionLogger tests the constructor
func TestNewPostgresTransactionLogger(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

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

// TestPostgresTransactionLogger_Run tests the Run method initialization
func TestPostgresTransactionLogger_Run(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer dbCleanup(t, db, mock)

	logger := &PostgresTransactionLogger{db: db}

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
		if ok && err != nil {
			t.Errorf("Expected no errors, got: %v", err)
		}
	default:
		// Expected - no errors
	}

	if len(events) != 0 {
		t.Errorf("Expected 0 events from empty table, got %d", len(events))
	}

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
		if ok && err != nil {
			t.Errorf("Expected no errors, got: %v", err)
		}
	default:
		// Expected - no errors
	}

	expectedEvents := []Event{
		{Sequence: 1, Kind: EventPut, Key: "key1", Value: "value1"},
		{Sequence: 2, Kind: EventDelete, Key: "key2", Value: ""},
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
		if err == nil {
			t.Error("Expected error for query failure")
		}
		if !strings.Contains(err.Error(), "failed to read transactions") {
			t.Errorf("Expected 'failed to read transactions' in error message, got: %v", err)
		}
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
		if err == nil {
			t.Error("Expected error for scan failure")
		}
		if !strings.Contains(err.Error(), "failed to read row") {
			t.Errorf("Expected 'failed to read row' in error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected error but got none")
	}

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
		if err == nil {
			t.Error("Expected error from failing write")
		}
		if !strings.Contains(err.Error(), "simulated write error") {
			t.Errorf("Expected 'simulated write error' in error message, got: %v", err)
		}
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

func dbCleanup(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock) {
	mock.ExpectClose()
	err := db.Close()
	assert.NoError(t, err)
}
