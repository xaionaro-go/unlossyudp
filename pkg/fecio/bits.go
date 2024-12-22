package fecio

func isPowerOfTwo(v uint32) bool {
	return v&(v-1) == 0
}
