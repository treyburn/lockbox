package store_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/treyburn/lockbox/store"
)

func TestPut(t *testing.T) {
	testStorage := make(map[string]string)
	s := store.NewInMemoryStore(store.WithStorage(testStorage))
	require.Empty(t, testStorage)

	err := s.Put("foo", "bar")
	assert.NoError(t, err)
	assert.Len(t, testStorage, 1)
	assert.Equal(t, testStorage["foo"], "bar")

	err = s.Put("foo", "baz")
	assert.NoError(t, err)
	assert.Len(t, testStorage, 1)
	assert.Equal(t, testStorage["foo"], "baz")

	err = s.Put("baz", "bing")
	assert.NoError(t, err)
	assert.Len(t, testStorage, 2)
	assert.Equal(t, testStorage["baz"], "bing")
}

func TestGet(t *testing.T) {
	testStorage := make(map[string]string)
	s := store.NewInMemoryStore(store.WithStorage(testStorage))
	require.Empty(t, testStorage)

	err := s.Put("foo", "bar")
	require.NoError(t, err)
	err = s.Put("bar", "baz")
	require.NoError(t, err)
	require.Len(t, testStorage, 2)

	got, err := s.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)

	got, err = s.Get("baz")
	assert.ErrorIs(t, store.ErrNotFound, err)
	assert.Empty(t, got)
}

func TestDelete(t *testing.T) {
	testStorage := make(map[string]string)
	s := store.NewInMemoryStore(store.WithStorage(testStorage))
	require.Empty(t, testStorage)

	err := s.Put("foo", "bar")
	require.NoError(t, err)
	require.Len(t, testStorage, 1)

	err = s.Delete("bar")
	assert.NoError(t, err)
	assert.Len(t, testStorage, 1)

	err = s.Delete("foo")
	assert.NoError(t, err)
	assert.Len(t, testStorage, 0)
}

func TestNewInMemoryStore(t *testing.T) {
	s := store.NewInMemoryStore()

	got, err := s.Get("foo")
	assert.ErrorIs(t, store.ErrNotFound, err)
	assert.Empty(t, got)

	err = s.Put("foo", "bar")
	assert.NoError(t, err)

	got, err = s.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)

	err = s.Delete("foo")
	assert.NoError(t, err)

	got, err = s.Get("foo")
	assert.ErrorIs(t, store.ErrNotFound, err)
	assert.Empty(t, got)
}

func TestWithStorage(t *testing.T) {
	testStorage := make(map[string]string)
	testStorage["foo"] = "bar"
	s := store.NewInMemoryStore(store.WithStorage(testStorage))

	got, err := s.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)
}
