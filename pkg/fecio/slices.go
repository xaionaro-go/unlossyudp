package fecio

import (
	"fmt"
	"sync"
)

func getSliceFromPool[T any](
	buffer *sync.Pool,
	size int,
) *[]T {
	buf := buffer.Get().(*[]T)
	if size < 0 {
		*buf = (*buf)[:cap(*buf)]
	}
	if cap(*buf) < size {
		*buf = make([]T, 0, size)
	} else {
		if size == -1 {
			*buf = (*buf)[:cap(*buf)]
		} else {
			*buf = (*buf)[:size]
		}
	}
	return buf
}

func copySlice[T any](
	in []T,
) []T {
	buf := make([]T, len(in))
	copy(buf, in)
	return buf
}

func copySliceUsingPool[T any](
	buffer *sync.Pool,
	in []T,
) *[]T {
	buf := getSliceFromPool[T](buffer, len(in))
	copy(*buf, in)
	return buf
}

func resizeSlice[T any](in []T, newSize, newMinCap int) []T {
	if newSize > newMinCap {
		panic(fmt.Errorf("the selected size is greater than the selected capacity: %d > %d", newSize, newMinCap))
	}
	if cap(in) >= newMinCap {
		return in[:newSize]
	}
	out := make([]T, newSize, newMinCap)
	copy(out, in)
	return out
}
