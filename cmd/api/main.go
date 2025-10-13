package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	api "github.com/treyburn/lockbox/internal/pkg/service/http"
	"github.com/treyburn/lockbox/internal/pkg/store"
)

//nolint:cyclop
func initializeLogger(cache store.Store) (store.TransactionLog, error) {
	// TODO - I believe this needs to be 0755 in order for the file to be shared between copies?
	file, err := os.OpenFile("/var/log/transaction.log", os.O_RDWR|os.O_APPEND, 0o755) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("error opening transaction log file: %w", err)
	}
	logger := store.NewTransactionLog(file)

	// db backed logger setup
	// logger, err := store.NewPostgresTransactionLogger(store.PostgresDBParams{
	// 	Host:     os.Getenv("POSTGRES_HOST"),
	// 	Port:     5432,
	// 	User:     os.Getenv("POSTGRES_USER"),
	// 	Password: os.Getenv("POSTGRES_PASSWORD"),
	// 	Database: os.Getenv("POSTGRES_DATABASE"),
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("error opening postgres transaction log: %w", err)
	// }

	events, errs := logger.ReadEvents()

	e, ok := store.Event{}, true
	for ok && err == nil {
		select {
		case e, ok = <-events:
			if !ok {
				// channel was closed
				break
			}
			slog.Debug(fmt.Sprintf("event: %+v", e))
			switch e.Kind {
			case store.EventPut:
				err = cache.Put(e.Key, e.Value)
			case store.EventDelete:
				err = cache.Delete(e.Key)
			default:
				err = fmt.Errorf("unknown event kind: %d", e.Kind)
			}
		case err, ok = <-errs:
			if !ok {
				// channel was closed
				break
			}
			slog.Error(fmt.Sprintf("error reading events: %v", err))
		}
	}

	if err != nil {
		return nil, fmt.Errorf("error processing events at logger startup: %w", err)
	}

	logger.Run()

	return logger, err
}

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info("Starting API server")
	cache := store.NewInMemoryStore()
	logger, err := initializeLogger(cache)
	if err != nil {
		slog.Error(fmt.Sprintf("error initializing logger: %v", err))
		os.Exit(1)
	}

	svc := api.NewService(cache, logger)
	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", svc.PutForKey).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", svc.GetByKey).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", svc.DeleteKey).Methods(http.MethodDelete)

	// example for handling https directly
	// const cert = "/etc/ssl/certs/app/cert.pem"
	// const key = "/etc/ssl/certs/app/key.pem"
	// err = http.ListenAndServeTLS(":8080", cert, key, r)

	// TODO - need to enable a sane default timeout
	err = http.ListenAndServe(":8080", r) //nolint:gosec
	if err != nil {
		slog.Error(fmt.Sprintf("serrver error: %v", err))
		os.Exit(1)
	}

	slog.Info("shutting down")
	os.Exit(0)
}
