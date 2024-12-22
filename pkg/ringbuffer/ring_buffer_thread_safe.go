package ringbuffer

import (
	"sync"
)

type RingBufferThreadSafe[T any] struct {
	locker sync.RWMutex
	*RingBuffer[T]
}

func NewThreadSafe[T any](
	size uint,
	onEvict func(T),
) *RingBufferThreadSafe[T] {
	return &RingBufferThreadSafe[T]{RingBuffer: New[T](size, onEvict)}
}

func (buf *RingBufferThreadSafe[T]) Set(idx int, item T) {
	buf.locker.Lock()
	defer buf.locker.Unlock()
	buf.RingBuffer.Set(idx, item)
}

func (buf *RingBufferThreadSafe[T]) Get(idx int) (T, bool) {
	buf.locker.RLock()
	defer buf.locker.RUnlock()
	return buf.RingBuffer.Get(idx)
}

func (buf *RingBufferThreadSafe[T]) Unset(idx int) {
	buf.locker.Lock()
	defer buf.locker.Unlock()
	buf.RingBuffer.Unset(idx)
}

func (buf *RingBufferThreadSafe[T]) SetBeginningIdx(idx int) {
	buf.locker.Lock()
	defer buf.locker.Unlock()
	buf.RingBuffer.SetBeginningIdx(idx)
}

func (buf *RingBufferThreadSafe[T]) SetEndingIdx(idx int) {
	buf.locker.Lock()
	defer buf.locker.Unlock()
	buf.RingBuffer.SetEndingIdx(idx)
}
