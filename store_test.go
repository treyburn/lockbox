package lockbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	require.Empty(t, store)
	defer func() {
		store = make(map[string]string)
	}()

	err := Put("foo", "bar")
	assert.NoError(t, err)
	assert.Len(t, store, 1)
	assert.Equal(t, store["foo"], "bar")

	err = Put("foo", "baz")
	assert.NoError(t, err)
	assert.Len(t, store, 1)
	assert.Equal(t, store["foo"], "baz")

	err = Put("baz", "bing")
	assert.NoError(t, err)
	assert.Len(t, store, 2)
	assert.Equal(t, store["baz"], "bing")
}

func TestGet(t *testing.T) {
	require.Empty(t, store)
	defer func() {
		store = make(map[string]string)
	}()

	err := Put("foo", "bar")
	require.NoError(t, err)
	err = Put("bar", "baz")
	require.NoError(t, err)
	require.Len(t, store, 2)

	got, err := Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", got)

	got, err = Get("baz")
	assert.ErrorIs(t, ErrNotFound, err)
	assert.Empty(t, got)
}

func TestDelete(t *testing.T) {
	require.Empty(t, store)
	defer func() {
		store = make(map[string]string)
	}()

	err := Put("foo", "bar")
	require.NoError(t, err)
	require.Len(t, store, 1)

	err = Delete("bar")
	assert.NoError(t, err)
	assert.Len(t, store, 1)

	err = Delete("foo")
	assert.NoError(t, err)
	assert.Len(t, store, 0)
}
