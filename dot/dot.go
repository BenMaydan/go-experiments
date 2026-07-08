package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"golang.org/x/exp/constraints"
	"math/rand"
	"runtime"
)


type MatMulConfig struct {
	// matrix is (H, W)
	// M and N are fractions of H and W respectively
	BlockSizeM int
	BlockSizeN int
}


func dot[T constraints.Integer](v1, v2 []T, start, end int, acc *atomic.Int32) {
	var sum int32 = 0
	for i := start; i < end; i++ {
		sum += int32(v1[i]) * int32(v2[i])
	}
	acc.Add(sum)
}


func Dot[T constraints.Integer](v1, v2 []T, ch chan int32) {
    if len(v1) != len(v2) {
        panic(fmt.Sprintf("Cannot do a dot product on vectors v1{%v} and v2{%v}", len(v1), len(v2)))
    }
    numWorkers := runtime.GOMAXPROCS(0)
    chunkSize := (len(v1) + numWorkers - 1) / numWorkers

    var wg sync.WaitGroup
    sum := atomic.Int32{}

    for w := range numWorkers {
        start := w * chunkSize
        end := min(start+chunkSize, len(v1))
        if start >= end {
            break
        }
        wg.Go(func() { dot(v1, v2, start, end, &sum) })
    }
    wg.Wait()
    ch <- sum.Load()
    close(ch)
}


func SeqDot[T constraints.Integer](v1, v2 []T) int32 {
	var sum int32 = 0
	for i := range len(v1) {
		sum += int32(v1[i]) * int32(v2[i])
	}
	return sum
}


func main() {
	fmt.Println(runtime.GOMAXPROCS(0))
	length := 10000000
	v1 := [10000000]int8{}
	v2 := [10000000]int8{}
	for i := range length {
		v1[i] = int8(rand.Int31n(100))
		v2[i] = int8(rand.Int31n(100))
	}

	fmt.Println("Sequential dot product:", SeqDot(v1[:], v2[:]))

	ch := make(chan int32)
	go Dot(v1[:], v2[:], ch)
	sum := <- ch

	fmt.Println("Parallel dot product:", sum)
}