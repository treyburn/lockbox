package logger

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Mutex-based counter
type MutexCounter struct {
	mu    sync.Mutex
	value uint64
}

func (c *MutexCounter) Increment() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
	return c.value
}

func (c *MutexCounter) Load() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Atomic-based counter
type AtomicCounter struct {
	value atomic.Uint64
}

func (c *AtomicCounter) Increment() uint64 {
	return c.value.Add(1)
}

func (c *AtomicCounter) Load() uint64 {
	return c.value.Load()
}

// Mixed workload with contention: 80% reads, 20% writes
func BenchmarkMutex_MixedWorkload(b *testing.B) {
	counter := &MutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkAtomic_MixedWorkload(b *testing.B) {
	counter := &AtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}

// Write-heavy workload: 50% reads, 50% writes
func BenchmarkMutex_WriteHeavy(b *testing.B) {
	counter := &MutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkAtomic_WriteHeavy(b *testing.B) {
	counter := &AtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}

// Read-heavy workload: 95% reads, 5% writes
func BenchmarkMutex_ReadHeavy(b *testing.B) {
	counter := &MutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkAtomic_ReadHeavy(b *testing.B) {
	counter := &AtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				counter.Increment()
			} else {
				_ = counter.Load()
			}
			i++
		}
	})
}