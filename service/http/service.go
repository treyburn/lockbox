package http

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/treyburn/lockbox/store"
)

func NewService(storage store.Store) *Service {
	return &Service{
		storage: storage,
	}
}

type Service struct {
	storage store.Store
}

func (s *Service) GetByKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := s.storage.Get(key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			slog.Warn(fmt.Sprintf("key not found: %v", key))
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			slog.Error(fmt.Sprintf("failed to read key: %v", err))
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(value))
	if err != nil {
		slog.Error(fmt.Sprintf("failed to write response: %v", err))
		return
	}
	slog.Debug(fmt.Sprintf("retrieved key: %v", key))
}

func (s *Service) PutForKey(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to read request: %v", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.storage.Put(key, string(value))
	if err != nil {
		slog.Error(fmt.Sprintf("failed to store key: %v", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	slog.Debug(fmt.Sprintf("stored key: %v", key))
}

func (s *Service) DeleteKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := s.storage.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		slog.Error(fmt.Sprintf("failed to delete key: %v", err))
		return
	}

	w.WriteHeader(http.StatusOK)
	slog.Debug(fmt.Sprintf("deleted key: %v", key))
}
