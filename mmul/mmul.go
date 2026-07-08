package main

import (
	"fmt"
	"runtime"
	"sync"
)


func subMatMul[T Number](
	m1 *RowMajorMatrix[T],
	m2 *ColMajorMatrix[T],
	out *RowMajorMatrix[T],
	rowsToProcess,
	colTileWidth,
	kTileHeight,
	goroutineNumber int,
) {
	for _, colTileStart := range tileStarts(m1.Height(), colTileWidth) {
		startRow := goroutineNumber * rowsToProcess
		colTileEnd := min(m2.Width(), colTileStart + colTileWidth)
		for k := 0; k < (m1.Width() + kTileHeight - 1) / kTileHeight; k++ {
			kStart := k * kTileHeight
			kEnd := min(kStart + kTileHeight, m1.Width())
			for row := startRow; row < min(m1.Height(), startRow + rowsToProcess); row++ {
				v1 := m1.RowSlice(row, kStart, kEnd)
				for col := colTileStart; col < colTileEnd; col++ {
					// now we set out[row, col] = dot(row, col)
					// we may need to tile further by K since these two vectors probably don't fit in L1 cache
					out.Acc(row, col, dot(v1, m2.ColSlice(col, kStart, kEnd)))
				}
			}
		}
	}
}


func (m1 *RowMajorMatrix[T]) Mul(m2 *ColMajorMatrix[T], colTileWidth, kTileHeight int) *RowMajorMatrix[T] {
	// here is the fun part, deciding how to split up the work per goroutine
	// we will have 32 goroutines for maximal work
	// how do we split up the dot product / accumulation to maximize reads from the matrix?
	// a sub block of data size (w, h) from matrix A

	if m1.Width() != m2.Height() {
		panic(fmt.Sprintf("Matrix sizes (%v, %v) and (%v, %v) for MatMul are incompatible!", m1.Height(), m1.Width(), m2.Height(), m2.Width()))
	}

	outHeight, outWidth := m1.Height(), m2.Width() // (A, B) @ (B, C) == (A, C)
	out := NewRowMajorMatrix[T](outHeight, outWidth)
	// out[i, j] = m1[i, :] dot m2[:, j]

	numGoroutines := min(runtime.GOMAXPROCS(0), m1.Height())
	rowsPerGoroutine := (m1.Height() + numGoroutines - 1) / numGoroutines

	// a tile of output will be 32x32
	// for each tile of output, each goroutine will handle a row of the output
	// this means a goroutine will multiply row Y of matrix A by all columns X of matrix B
	// this means the input tile width will be tunable
	//		goroutines won't start up and shut down
	// instead, goroutine 0 will handle row [0, 1, 2, ..., 31] of matrix A

	var wg sync.WaitGroup

	for goroutineNumber := range numGoroutines {
		wg.Go(func() { subMatMul(m1, m2, out, rowsPerGoroutine, colTileWidth, kTileHeight, goroutineNumber) })
	}

	wg.Wait()

	return out
}

func (m1 *RowMajorMatrix[T]) SeqMul(m2 *ColMajorMatrix[T]) *RowMajorMatrix[T] {

	if m1.Width() != m2.Height() {
		panic(fmt.Sprintf("Matrix sizes (%v, %v) and (%v, %v) for MatMul are incompatible!", m1.Height(), m1.Width(), m2.Height(), m2.Width()))
	}

	outHeight, outWidth := m1.Height(), m2.Width() // (A, B) @ (B, C) == (A, C)
	out := NewRowMajorMatrix[T](outHeight, outWidth)
	// out[i, j] = m1[i, :] dot m2[:, j]

	for i := 0; i < outHeight; i++ {
		for j := 0; j < outWidth; j++ {
			var acc T
			for k := 0; k < m1.Width(); k++ {
				acc += m1.At(i, k) * m2.At(k, j)
			}
			out.Set(i, j, acc)
		}
	}

	return out
}

func main() {
	M, N, W := 2, 3, 2
	type T = int8
	A := NewRowMajorMatrix[T](M, N)
	B := NewColMajorMatrix[T](N, W)

	A.Set(0, 0, 1)
	A.Set(0, 1, 2)
	A.Set(0, 2, 3)
	A.Set(1, 0, 4)
	A.Set(1, 1, 5)
	A.Set(1, 2, 6)

	B.Set(0, 0, 0)
	B.Set(0, 1, 1)
	B.Set(1, 0, 1)
	B.Set(1, 1, 0)
	B.Set(2, 0, 0)
	B.Set(2, 1, 1)

	out := A.SeqMul(B)
	outParallel := A.Mul(B, 32, 32)

	fmt.Println(A)
	fmt.Println(B)
	fmt.Println(out)
	fmt.Println(outParallel)
}
