package dot

import (
	"fmt"
	"math/rand"
	"testing"
	"golang.org/x/exp/constraints"
	"unsafe"
)

func setupVectors[T constraints.Integer](n int, limit int32) ([]T, []T) {
	v1 := make([]T, n)
	v2 := make([]T, n)
	for i := 0; i < n; i++ {
		v1[i] = T(rand.Int31n(limit))
		v2[i] = T(rand.Int31n(limit))
	}
	return v1, v2
}

func BenchmarkSeqDot(b *testing.B) {
	size := 10_000_000
	v1, v2 := setupVectors[int8](size, 100)
	b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
		b.SetBytes(int64(len(v1)) * int64(unsafe.Sizeof(v1[0])) * 2) // 2 vectors, Sizeof bytes/elem — gives you MB/s
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			SeqDot(v1, v2)
		}
	})
}

func BenchmarkParallelDotSweep(b *testing.B) {
	const size = 10_000_000
	v1, v2 := setupVectors[int8](size, 100)

	b.Run(fmt.Sprintf("size=%v", size), func(b *testing.B) {
		// conf := DotProductConfig{BlockSize: block}
		b.SetBytes(int64(len(v1)) * int64(unsafe.Sizeof(v1[0])) * 2) // 2 vectors, Sizeof bytes/elem — gives you MB/s
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ch := make(chan int32)
			go Dot(v1, v2, ch)
			<-ch
		}
	})
}