package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	api "github.com/treyburn/lockbox/service/http"
	"github.com/treyburn/lockbox/store"
)

func initializeLogger(cache store.Store) (store.TransactionLog, error) {
	logger, err := store.NewTransactionLog("transaction.log")
	if err != nil {
		return nil, fmt.Errorf("error creating transaction logger: %w", err)
	}

	events, errs := logger.ReadEvents()

	e, ok := store.Event{}, true
	for ok && err == nil {
		select {
		case e, ok = <-events:
			slog.Debug(fmt.Sprintf("event: %v", e.Kind))
			switch e.Kind {
			case store.EventPut:
				err = cache.Put(e.Key, e.Value)
			case store.EventDelete:
				err = cache.Delete(e.Key)
			default:
				err = fmt.Errorf("unknown event kind: %d", e.Kind)
			}
		case err, ok = <-errs:
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

	err = http.ListenAndServe(":8080", r)
	if err != nil {
		slog.Error(fmt.Sprintf("serrver error: %v", err))
		os.Exit(1)
	}

	slog.Info("shutting down")
	os.Exit(0)
}
