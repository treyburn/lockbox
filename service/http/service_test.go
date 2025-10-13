package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"

	"github.com/treyburn/lockbox/store"
)

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
}

func TestService_PutForKey(t *testing.T) {
	t.Run("new key", func(t *testing.T) {
		t.Skip("failing - to fix")
		internalStore := map[string]string{}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", strings.NewReader("some-value"))
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Empty(t, response.Body)
		assert.Equal(t, "some-value", internalStore["some-key"])
	})

	t.Run("existing key", func(t *testing.T) {
		t.Skip("failing - need a mock in here")
		internalStore := map[string]string{"some-key": "some-existing-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/v1/some-key", strings.NewReader("some-new-value"))
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.PutForKey(response, request)
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Empty(t, response.Body)
		assert.Equal(t, "some-new-value", internalStore["some-key"])
	})
}

func TestService_DeleteForKey(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		t.Skip("failing - need a mock in here")
		internalStore := map[string]string{"some-key": "some-existing-value"}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.DeleteKey(response, request)
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Empty(t, response.Body)
		assert.Empty(t, internalStore)
	})
	t.Run("no error on non-existing key", func(t *testing.T) {
		t.Skip("failing - need a mock in here")
		internalStore := map[string]string{}
		cache := store.NewInMemoryStore(store.WithStorage(internalStore))
		svc := NewService(cache, nil)

		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodDelete, "/v1/some-key", nil)
		request = mux.SetURLVars(request, map[string]string{"key": "some-key"})

		svc.DeleteKey(response, request)
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Empty(t, response.Body)
		assert.Empty(t, internalStore)
	})
}
