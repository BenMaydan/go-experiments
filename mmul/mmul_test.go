package main

import (
	"math/rand"
	"testing"
	"fmt"
)

// --- helpers -----------------------------------------------------------

func randomRowMajor[T Number](rows, cols int, rng *rand.Rand) *RowMajorMatrix[T] {
	m := NewRowMajorMatrix[T](rows, cols)
	for i := range rows {
		for j := range cols {
			// m.Set(i, j, rng.Float64()*20-10) // values in [-10, 10)
			m.Set(i, j, T(rng.Float64()*20-10))
		}
	}
	return m
}

func randomColMajor[T Number](rows, cols int, rng *rand.Rand) *ColMajorMatrix[T] {
	m := NewColMajorMatrix[T](rows, cols)
	for i := range rows {
		for j := range cols {
			m.Set(i, j, T(rng.Float64()*20-10))
		}
	}
	return m
}

func matricesEqual[T Number](a, b *RowMajorMatrix[T], tol float64) bool {
	if a.Height() != b.Height() || a.Width() != b.Width() {
		return false
	}
	for i := 0; i < a.Height(); i++ {
		for j := 0; j < a.Width(); j++ {
			diff := a.At(i, j) - b.At(i, j)
			if diff < 0 {
				diff = -diff
			}
			if float64(diff) > tol {
				return false
			}
		}
	}
	return true
}

// --- correctness ---------------------------------------------------------

func TestParallelMatchesSequential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	sizes := []struct {
		m, k, n int
	}{
		{1, 1, 1},
		{2, 3, 2},   // matches the shape used in main()
		{5, 7, 3},
		{16, 16, 16},
		{31, 33, 17}, // deliberately not divisible by tile width
		{64, 128, 64},
		{100, 50, 75},
	}

	tileWidths := []int{1, 8, 32, 64}

	for _, sz := range sizes {
		for _, tile := range tileWidths {
			A := randomRowMajor[int](sz.m, sz.k, rng)
			B := randomColMajor[int](sz.k, sz.n, rng)

			want := A.SeqMul(B)
			got := A.Mul(B, tile, A.Width())

			if !matricesEqual(want, got, 1e-9) {
				t.Errorf(
					"mismatch for m=%d k=%d n=%d tileWidth=%d: parallel result did not match sequential result",
					sz.m, sz.k, sz.n, tile,
				)
			}
		}
	}
}

func TestParallelMatchesSequentialKnownValues(t *testing.T) {
	// same example as main(), but with a type that supports fractional
	// comparisons cleanly (float64) so we can reuse matricesEqual
	A := NewRowMajorMatrix[float64](2, 3)
	A.Set(0, 0, 1)
	A.Set(0, 1, 2)
	A.Set(0, 2, 3)
	A.Set(1, 0, 4)
	A.Set(1, 1, 5)
	A.Set(1, 2, 6)

	B := NewColMajorMatrix[float64](3, 2)
	B.Set(0, 0, 0)
	B.Set(0, 1, 1)
	B.Set(1, 0, 1)
	B.Set(1, 1, 0)
	B.Set(2, 0, 0)
	B.Set(2, 1, 1)

	want := A.SeqMul(B)
	got := A.Mul(B, 32, A.Width())

	if !matricesEqual(want, got, 1e-9) {
		t.Errorf("known-value case mismatch:\nseq:\n%v\npar:\n%v", want, got)
	}
}

// --- benchmarks ------------------------------------------------------------
//
// Run with:
//   go test -bench=. -benchmem
// or to isolate one:
//   go test -bench=BenchmarkParallelMul -benchmem

var benchSizes = []struct {
	name    string
	m, k, n int
}{
	{"64x64x64", 64, 64, 64},
	{"128x128x128", 128, 128, 128},
	{"256x256x256", 256, 256, 256},
	{"512x512x512", 512, 512, 512},
	{"1024x1024x1024", 1024, 1024, 1024},
	{"2048x2048x2048", 1024, 1024, 1024},
}

func BenchmarkSeqMul(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	for _, sz := range benchSizes {
		A := randomRowMajor[int](sz.m, sz.k, rng)
		B := randomColMajor[int](sz.k, sz.n, rng)
		b.Run(sz.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = A.SeqMul(B)
			}
		})
	}
}

func BenchmarkParallelMul(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	tileWidths := []int{8, 16, 32, 64, 128}
	kWidths := []int{8, 16, 32, 64}

	for _, sz := range benchSizes {
		A := randomRowMajor[float32](sz.m, sz.k, rng)
		B := randomColMajor[float32](sz.k, sz.n, rng)
		for _, tile := range tileWidths {
			for _, k := range kWidths {
				name := sz.name + "/tile=" + fmt.Sprint(tile) + "/k=" + fmt.Sprint(k)
				b.Run(name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_ = A.Mul(B, tile, k)
					}
				})
			}
		}
	}
}