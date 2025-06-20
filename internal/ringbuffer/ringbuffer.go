package ringbuffer

import (
	"sync"
)

type RingBuffer[T any] struct {
	size  int
	off   int
	count int
	data  []T
	mu    sync.Mutex
}

func NewRingBuffer[T any](size int) *RingBuffer[T] {
	return &RingBuffer[T]{
		size: size,
		data: make([]T, size),
	}
}

func (r *RingBuffer[T]) Append(d T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[r.off] = d
	r.off = (r.off + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

func (r *RingBuffer[T]) GetAll(dst []T) []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == 0 {
		var zero []T
		return zero
	}
	c := r.count
	off := r.off
	for c > 0 {
		c--
		dst = append(dst, r.data[off])
		off = (off - 1 + r.size) % r.size
	}
	return dst
}

func (r *RingBuffer[T]) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}
