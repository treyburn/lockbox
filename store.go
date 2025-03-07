package lockbox

import "errors"

var store = make(map[string]string)

var ErrNotFound = errors.New("key not found")

func Put(key, value string) error {
	store[key] = value
	return nil
}

func Get(key string) (string, error) {
	value, ok := store[key]
	if !ok {
		return "", ErrNotFound
	}

	return value, nil
}

func Delete(key string) error {
	delete(store, key)
	return nil
}
