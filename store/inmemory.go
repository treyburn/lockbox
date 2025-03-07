package store

func NewInMemoryStore(opts ...InMemoryOption) *InMemoryStore {
	store := &InMemoryStore{
		store: make(map[string]string),
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

type InMemoryStore struct {
	store map[string]string
}

func (s *InMemoryStore) Put(key, value string) error {
	s.store[key] = value
	return nil
}

func (s *InMemoryStore) Get(key string) (string, error) {
	value, ok := s.store[key]
	if !ok {
		return "", ErrNotFound
	}

	return value, nil
}

func (s *InMemoryStore) Delete(key string) error {
	delete(s.store, key)
	return nil
}

type InMemoryOption = func(*InMemoryStore)

func WithStorage(storage map[string]string) InMemoryOption {
	return func(store *InMemoryStore) {
		store.store = storage
	}
}
