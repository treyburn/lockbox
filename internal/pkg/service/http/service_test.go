package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/treyburn/lockbox/internal/pkg/store"
)

type mockTransactionLog struct {
	mock.Mock
}

func (m *mockTransactionLog) WritePut(key, value string) {
	m.Called(key, value)
}

func (m *mockTransactionLog) WriteDelete(key string) {
	m.Called(key)
}

type errorStore struct {
	err error
}

func (e *errorStore) Get(_ string) (string, error) { return "", e.err }
func (e *errorStore) Put(_, _ string) error        { return e.err }
func (e *errorStore) Delete(_ string) error        { return e.err }

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errors.New("read error")
}

func TestService_GetByKey(t *testing.T) {
	t.Run("found key", func(t *testing.T) {
		internalStore := map[string]string{"some-key": "some-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.GetByKey(response, request)
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "some-value", response.Body.String())
	})

	t.Run("not found key", func(t *testing.T) {
		internalStore := map[string]string{"some-key": "some-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/some-other-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-other-key"})

		svc.GetByKey(response, request)
		assert.Equal(t, http.StatusNotFound, response.Code)
		assert.Empty(t, response.Body)
	})

	t.Run("internal error", func(t *testing.T) {
		s := &errorStore{err: errors.New("db error")}
		svc := NewService(s, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.GetByKey(response, request)
		assert.Equal(t, http.StatusInternalServerError, response.Code)
	})
}

func TestService_PutForKey(t *testing.T) {
	t.Run("new key", func(t *testing.T) {
		internalStore := map[string]string{}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		txLog := &mockTransactionLog{}
		txLog.On("WritePut", "some-key", "some-value").Return()
		svc := NewService(cache, txLog)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", strings.NewReader("some-value"))
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "some-value", internalStore["some-key"])
		txLog.AssertExpectations(t)
	})

	t.Run("existing key", func(t *testing.T) {
		internalStore := map[string]string{"some-key": "some-existing-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		txLog := &mockTransactionLog{}
		txLog.On("WritePut", "some-key", "some-new-value").Return()
		svc := NewService(cache, txLog)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", strings.NewReader("some-new-value"))
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "some-new-value", internalStore["some-key"])
		txLog.AssertExpectations(t)
	})

	t.Run("read body error", func(t *testing.T) {
		cache := store.NewInMemoryStore()
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", errReader{})
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusInternalServerError, response.Code)
	})

	t.Run("store error", func(t *testing.T) {
		s := &errorStore{err: errors.New("db error")}
		svc := NewService(s, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", strings.NewReader("some-value"))
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusInternalServerError, response.Code)
	})
}

func TestService_DeleteForKey(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		internalStore := map[string]string{"some-key": "some-existing-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		txLog := &mockTransactionLog{}
		txLog.On("WriteDelete", "some-key").Return()
		svc := NewService(cache, txLog)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.DeleteKey(response, request)
		assert.Equal(t, http.StatusAccepted, response.Code)
		assert.Empty(t, internalStore)
		txLog.AssertExpectations(t)
	})
	t.Run("no error on non-existing key", func(t *testing.T) {
		internalStore := map[string]string{}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		txLog := &mockTransactionLog{}
		txLog.On("WriteDelete", "some-key").Return()
		svc := NewService(cache, txLog)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.DeleteKey(response, request)
		assert.Equal(t, http.StatusAccepted, response.Code)
		assert.Empty(t, internalStore)
		txLog.AssertExpectations(t)
	})

	t.Run("store error", func(t *testing.T) {
		s := &errorStore{err: errors.New("db error")}
		svc := NewService(s, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.DeleteKey(response, request)
		assert.Equal(t, http.StatusInternalServerError, response.Code)
	})
}
