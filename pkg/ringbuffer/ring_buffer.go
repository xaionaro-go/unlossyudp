package ringbuffer

type Range struct {
	StartIdx int
	EndIdx   int
}

type RingBuffer[T any] struct {
	Buffer          []T
	IsSet           []bool
	OnEvict         func(T)
	CurrentBeginIdx int
}

func New[T any](
	size uint,
	onEvict func(T),
) *RingBuffer[T] {
	return &RingBuffer[T]{
		Buffer:  make([]T, size),
		IsSet:   make([]bool, size),
		OnEvict: onEvict,
	}
}

func (buf *RingBuffer[T]) Set(idx int, item T) {
	if idx < buf.CurrentBeginIdx {
		buf.SetBeginningIdx(idx)
	}

	endIdx := buf.CurrentBeginIdx + len(buf.Buffer)
	if idx >= endIdx {
		buf.SetEndingIdx(endIdx)
	}

	buf.setIgnoreRange(idx, item)
}

func (buf *RingBuffer[T]) setIgnoreRange(idx int, item T) {
	bufIdx := buf.getBufIdx(idx)
	buf.Buffer[bufIdx] = item
	buf.IsSet[bufIdx] = true
}

func (buf *RingBuffer[T]) Get(idx int) (T, bool) {
	if idx < buf.CurrentBeginIdx {
		var zeroValue T
		return zeroValue, false
	}
	endIdx := buf.CurrentBeginIdx + len(buf.Buffer)
	if idx >= endIdx {
		var zeroValue T
		return zeroValue, false
	}

	bufIdx := idx % len(buf.Buffer)
	if !buf.IsSet[bufIdx] {
		var zeroValue T
		return zeroValue, false
	}
	return buf.Buffer[bufIdx], true
}

func (buf *RingBuffer[T]) Unset(idx int) {
	if idx < buf.CurrentBeginIdx {
		return
	}
	endIdx := buf.CurrentBeginIdx + len(buf.Buffer)
	if idx >= endIdx {
		return
	}

	buf.unsetIgnoreRange(idx)
}

func (buf *RingBuffer[T]) SetBeginningIdx(idx int) {
	diff := idx - buf.CurrentBeginIdx
	buf.CurrentBeginIdx = idx
	switch {
	case diff == 0:
	case diff < 0:
		for i := range -diff {
			buf.unsetIgnoreRange(idx + i)
		}
	case diff > 0:
		for i := range diff {
			buf.unsetIgnoreRange(idx - i)
		}
	}
}

func (buf *RingBuffer[T]) SetEndingIdx(endIdx int) {
	buf.SetBeginningIdx(endIdx - len(buf.Buffer) - 1)
}

func (buf *RingBuffer[T]) getBufIdx(idx int) int {
	negativeRounds := -(idx - len(buf.Buffer) + 1) / len(buf.Buffer)
	if negativeRounds < 0 {
		negativeRounds = 0
	}
	idx += negativeRounds * len(buf.Buffer)
	return idx % len(buf.Buffer)
}

func (buf *RingBuffer[T]) unsetIgnoreRange(idx int) {
	bufIdx := buf.getBufIdx(idx)
	if !buf.IsSet[bufIdx] {
		return
	}
	if buf.OnEvict != nil {
		buf.OnEvict(buf.Buffer[bufIdx])
	}
	buf.IsSet[bufIdx] = false

	// Keep in mind, we don't reset the buf.Buffer[bufIdx] value here,
	// and if it holds a pointer, GC won't free that memory, until
	// the value is overwritten.
}
