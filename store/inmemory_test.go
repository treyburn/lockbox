package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	store := NewInMemoryStore()
	require.Empty(t, store.store)

	err := store.Put("foo", "bar")
	assert.NoError(t, err)
	assert.Len(t, store, 1)
	assert.Equal(t, store.store["foo"], "bar")

	err = store.Put("foo", "baz")
	assert.NoError(t, err)
	assert.Len(t, store, 1)
	assert.Equal(t, store.store["foo"], "baz")

	err = store.Put("baz", "bing")
	assert.NoError(t, err)
	assert.Len(t, store, 2)
	assert.Equal(t, store.store["baz"], "bing")
}

func TestGet(t *testing.T) {
	store := NewInMemoryStore()
	require.Empty(t, store.store)

	err := store.Put("foo", "bar")
	require.NoError(t, err)
	err = store.Put("bar", "baz")
	require.NoError(t, err)
	require.Len(t, store, 2)

	got, err := store.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)

	got, err = store.Get("baz")
	assert.ErrorIs(t, ErrNotFound, err)
	assert.Empty(t, got)
}

func TestDelete(t *testing.T) {
	store := NewInMemoryStore()
	require.Empty(t, store.store)

	err := store.Put("foo", "bar")
	require.NoError(t, err)
	require.Len(t, store, 1)

	err = store.Delete("bar")
	assert.NoError(t, err)
	assert.Len(t, store, 1)

	err = store.Delete("foo")
	assert.NoError(t, err)
	assert.Len(t, store, 0)
}
