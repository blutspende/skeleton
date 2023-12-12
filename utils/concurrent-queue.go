package utils

import (
	"context"
	"sync"
	"time"
)

// Generic Struct
type ConcurrentQueue[T any] struct {
	// array of items
	items []T
	// Mutual exclusion lock
	lock sync.Mutex
	// Cond is used to pause multiple goroutines and wait
	cond *sync.Cond
}

// Initialize ConcurrentQueue
func NewConcurrentQueue[T any](ctx context.Context) *ConcurrentQueue[T] {
	q := &ConcurrentQueue[T]{}
	q.cond = sync.NewCond(&q.lock)

	go func() {
		for {
			select {
			case <-ctx.Done():
				q.cond.Broadcast()
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return q
}

// Put the item in the queue
func (q *ConcurrentQueue[T]) Enqueue(item T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = append(q.items, item)
	// Cond signals other go routines to execute
	q.cond.Signal()
}

// Gets the item from queue
func (q *ConcurrentQueue[T]) Dequeue() T {
	q.lock.Lock()
	defer q.lock.Unlock()
	// if Get is called before Put, then cond waits until the Put signals.
	for len(q.items) == 0 {
		q.cond.Wait()
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *ConcurrentQueue[T]) IsEmpty() bool {
	return len(q.items) == 0
}
