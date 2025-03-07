package store

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		store: make(map[string]string),
	}
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
