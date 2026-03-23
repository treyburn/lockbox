package logger

type TransactionLog interface {
	WritePut(key, value string)
	WriteDelete(key string)
}

type TransactionManager interface {
	TransactionLog

	Run()
	ReadEvents() (<-chan Event, <-chan error)
	Err() <-chan error
	Close() error
}
