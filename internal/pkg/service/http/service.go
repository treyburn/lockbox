package http

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/treyburn/lockbox/internal/pkg/logger"
	"github.com/treyburn/lockbox/internal/pkg/store"
)

func NewService(storage store.Store, logger logger.TransactionLog) *Service {
	return &Service{
		storage: storage,
		logger:  logger,
	}
}

type Service struct {
	storage store.Store
	logger  logger.TransactionLog
}

func (s *Service) GetByKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := s.storage.Get(key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			slog.Warn("key not found", slog.String("key", strconv.Quote(key)))
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			slog.Error("failed to read key", slog.String("key", strconv.Quote(key)), slog.Any("error", err))
		}
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(value)) //nolint:gosec // Content-Type set to application/octet-stream which prevents XSS
	if err != nil {
		slog.Error("failed to write response", slog.Any("error", err))
		return
	}
	slog.Debug("retrieved key", slog.String("key", strconv.Quote(key)))
}

func (s *Service) PutForKey(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			slog.Error("failed to close request body", slog.Any("error", err))
		}
	}()
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("failed to read request", slog.Any("error", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.storage.Put(key, string(value))
	if err != nil {
		slog.Error("failed to store key", slog.Any("error", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
	slog.Debug("stored key", slog.String("key", strconv.Quote(key)))
}

func (s *Service) DeleteKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := s.storage.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		slog.Error("failed to delete key", slog.Any("error", err))
		return
	}

	s.logger.WriteDelete(key)
	w.WriteHeader(http.StatusAccepted)
	slog.Debug("deleted key", slog.String("key", strconv.Quote(key)))
}
