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

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info("Starting API server")
	cache := store.NewInMemoryStore()
	svc := api.NewService(cache)
	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", svc.PutForKey).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", svc.GetByKey).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", svc.DeleteKey).Methods(http.MethodDelete)

	err := http.ListenAndServe(":8080", r)
	if err != nil {
		slog.Error(fmt.Sprintf("serrver error: %v", err))
		os.Exit(1)
	}

	slog.Info("shutting down")
	os.Exit(0)
}
