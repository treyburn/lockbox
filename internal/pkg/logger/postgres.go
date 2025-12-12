package logger

import (
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/lib/pq"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

type PostgresDBParams struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewPostgresTransactionLogger(conf PostgresDBParams) (*PostgresTransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", conf.Host, conf.Port, conf.User, conf.Password, conf.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db handle: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	p := &PostgresTransactionLogger{db: db}

	exists, err := p.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}

	if !exists {
		if err := p.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return p, nil
}

func (p *PostgresTransactionLogger) WritePut(key, value string) {
	p.events <- Event{Kind: EventPut, Key: key, Value: value}
}

func (p *PostgresTransactionLogger) WriteDelete(key string) {
	p.events <- Event{Kind: EventDelete, Key: key}
}

func (p *PostgresTransactionLogger) Err() <-chan error {
	return p.errors
}

func (p *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	p.events = events
	errs := make(chan error, 1)
	p.errors = errs

	const insertQuery = `INSERT INTO transactions
					(event_type, key, value)
					VALUES ($1, $2, $3)`

	go func() {
		for e := range events {

			_, err := p.db.Exec(insertQuery, e.Kind, e.Key, e.Value)
			if err != nil {
				errs <- fmt.Errorf("failed to write transaction: %w", err)
			}
		}
	}()
}

func (p *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outErr := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outErr)

		const query = `SELECT sequence, event_type, key, value FROM transactions
						ORDER BY sequence`

		rows, err := p.db.Query(query)
		if err != nil {
			outErr <- fmt.Errorf("failed to read transactions: %w", err)
			return
		}
		defer func() {
			closeErr := rows.Close()
			if closeErr != nil {
				slog.Warn("failed to close db row", slog.String("error", closeErr.Error()))
			}
		}()
		e := Event{}
		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.Kind, &e.Key, &e.Value)
			if err != nil {
				outErr <- fmt.Errorf("failed to read row: %w", err)
			}
			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outErr <- fmt.Errorf("failed to read rows: %w", err)
		}
	}()

	return outEvent, outErr
}

func (p *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result sql.NullString

	rows, err := p.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s');", table))
	defer func() {
		if rows != nil {
			if closeErr := rows.Close(); closeErr != nil {
				slog.Warn("failed to close db row", slog.String("error", closeErr.Error()))
			}
		}
	}()
	if err != nil {
		return false, err
	}

	for rows.Next() && result.String != table {
		err = rows.Scan(&result)
		if err != nil {
			return false, err
		}
	}

	return result.String == table, rows.Err()
}

func (p *PostgresTransactionLogger) createTable() error {
	const createQuery = `CREATE TABLE transactions (
		sequence      BIGSERIAL PRIMARY KEY,
		event_type    SMALLINT,
		key 		  TEXT,
		value         TEXT
	  );`

	_, err := p.db.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (p *PostgresTransactionLogger) Close() error {
	close(p.events)
	return p.db.Close()
}
