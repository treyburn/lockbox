package bench

import (
	"sync"
	"sync/atomic"
	"testing"
)

// The goal of these benchmarks was to determine whether we should use a [sync.Mutex], [sync.RWMutex], or an
// [atomic.Uint64] as our synchronizing primitive for the sequence counter in the [logger.FileTransactionLogger].
// The results showed a performance improvement across the board for an [atomic.Uint64] - even in cases where we may
// have heavy write contention - but especially so where we had heavy read traffic.
//
// Benchmark Results (AMD Ryzen 7 9800X3D 8-Core Processor)
// ┌────────────────────────────┬──────────────┐
// │ Benchmark                  │       ns/op  │
// ├────────────────────────────┼──────────────┤
// │ Mixed Workload             │              │
// │   Mutex                    │       54.10  │
// │   RWMutex                  │       16.58  │
// │   Atomic                   │       10.08  │
// ├────────────────────────────┼──────────────┤
// │ Write Heavy                │              │
// │   Mutex                    │       56.98  │
// │   RWMutex                  │       22.79  │
// │   Atomic                   │       20.44  │
// ├────────────────────────────┼──────────────┤
// │ Read Heavy                 │              │
// │   Mutex                    │       53.37  │
// │   RWMutex                  │       11.37  │
// │   Atomic                   │        3.79  │
// └────────────────────────────┴──────────────┘

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

type RWMutexCounter struct {
	mu    sync.RWMutex
	value uint64
}

func (c *RWMutexCounter) Increment() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
	return c.value
}

func (c *RWMutexCounter) Load() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// Atomic-based counter
type AtomicCounter struct {
	value atomic.Uint64
}

func (c *AtomicCounter) Increment() uint64 {
	for {
		x := c.value.Load()
		if c.value.CompareAndSwap(x, x+1) {
			return x + 1
		}
	}
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
			switch i % 5 {
			case 0:
				_ = counter.Increment()
			default:
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkRWMutex_MixedWorkload(b *testing.B) {
	counter := &RWMutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 5 {
			case 0:
				_ = counter.Increment()
			default:
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
			switch i % 5 {
			case 0:
				_ = counter.Increment()
			default:
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
			switch i % 2 {
			case 0:
				_ = counter.Increment()
			default:
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkRWMutex_WriteHeavy(b *testing.B) {
	counter := &RWMutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 2 {
			case 0:
				_ = counter.Increment()
			default:
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
			switch i % 2 {
			case 0:
				_ = counter.Increment()
			default:
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
			switch i % 20 {
			case 0:
				_ = counter.Increment()
			default:
				_ = counter.Load()
			}
			i++
		}
	})
}

func BenchmarkRWMutex_ReadHeavy(b *testing.B) {
	counter := &RWMutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 20 {
			case 0:
				_ = counter.Increment()
			default:
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
			switch i % 20 {
			case 0:
				_ = counter.Increment()
			default:
				_ = counter.Load()
			}
			i++
		}
	})
}
