package mmul

import (
	"fmt"
	"golang.org/x/exp/constraints"
	// "github.com/tphakala/simd/i8"
)


type Number interface {
	constraints.Integer | constraints.Float
}

func numDigits[T Number](num T) int {
	return len(fmt.Sprint(num))
}

func Sum[T Number](slice []T) T {
	var total T
	for _, v := range slice {
		total += v
	}
	return total
}

func tileStarts(num, tileWidth int) []int {
	numChunks := (num + tileWidth - 1) / tileWidth
	tileStarts := make([]int, numChunks)
	for i := range numChunks {
		tileStarts[i] = i * tileWidth
	}
	return tileStarts
}

func dot[T Number](v1 , v2 []T) T {
	var sum T = 0
	for i := range len(v1) {
		sum += v1[i] * v2[i]
	}
	return sum
}