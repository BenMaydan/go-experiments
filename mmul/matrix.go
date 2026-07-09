package mmul

import (
	"fmt"
	"strings"
	"slices"
)


type Matrix[T Number] interface {
	Height() int
	Width() int
	At(row, col int) T
	Set(row, col int, val T)
	Acc(row, col int, val T)
	Min() T
	Max() T
	Col(col int) []T
	Row(row int) []T
}

type RowMajorMatrix[T Number] struct {
	Data []T
	numRows    int
	numCols    int
}

type ColMajorMatrix[T Number] struct {
	Data []T
	numRows    int
	numCols    int
}

func NewRowMajorMatrix[T Number](height, width int) *RowMajorMatrix[T] {
	return &RowMajorMatrix[T]{
		Data: make([]T, width*height),
		numRows:    height,
		numCols:    width,
	}
}

func NewColMajorMatrix[T Number](height, width int) *ColMajorMatrix[T] {
	return &ColMajorMatrix[T]{
		Data: make([]T, width*height),
		numRows:    height,
		numCols:    width,
	}
}

func (m *RowMajorMatrix[T]) Height() int {
	return m.numRows
}

func (m *ColMajorMatrix[T]) Height() int {
	return m.numRows
}

func (m *RowMajorMatrix[T]) Width() int {
	return m.numCols
}

func (m *ColMajorMatrix[T]) Width() int {
	return m.numCols
}

func (m *RowMajorMatrix[T]) At(row, col int) T {
	return m.Data[row*m.numCols+col]
}

func (m *ColMajorMatrix[T]) At(row, col int) T {
	return m.Data[col*m.numRows+row]
}

func (m *RowMajorMatrix[T]) Set(row, col int, val T) {
	m.Data[row*m.numCols+col] = val
}

func (m *RowMajorMatrix[T]) Acc(row, col int, val T) {
	m.Data[row*m.numCols+col] += val
}

func (m *ColMajorMatrix[T]) Set(row, col int, val T) {
	m.Data[col*m.numRows+row] = val
}

func (m *ColMajorMatrix[T]) Acc(row, col int, val T) {
	m.Data[col*m.numRows+row] += val
}

func (m *RowMajorMatrix[T]) Max() T {
	return slices.Max(m.Data)
}

func (m *ColMajorMatrix[T]) Max() T {
	return slices.Max(m.Data)
}

func (m *RowMajorMatrix[T]) Min() T {
	return slices.Min(m.Data)
}

func (m *ColMajorMatrix[T]) Min() T {
	return slices.Min(m.Data)
}

func (m *RowMajorMatrix[T]) Col(col int) []T {
	ret := make([]T, m.numRows)
	for row := range m.numRows {
		ret[row] = m.At(row, col)
	}
	return ret
}

func (m *RowMajorMatrix[T]) ColSlice(col, startRow, endRow int) []T {
	ret := make([]T, m.numRows)
	for row := startRow; row < endRow; row++ {
		ret[row] = m.At(row, col)
	}
	return ret
}

func (m *ColMajorMatrix[T]) Col(col int) []T {
	start := col * m.numRows
	return m.Data[start : start+m.numRows]
}

func (m *ColMajorMatrix[T]) ColSlice(col, startRow, endRow int) []T {
	start := col * m.numRows
	return m.Data[start + startRow: start + endRow]
}

func (m *RowMajorMatrix[T]) Row(row int) []T {
	start := row * m.numCols
	return m.Data[start : start+m.numCols]
}

func (m *RowMajorMatrix[T]) RowSlice(row, colStart, colEnd int) []T {
	start := row * m.numCols
	return m.Data[start + colStart : start + colEnd]
}

func (m *ColMajorMatrix[T]) Row(row int) []T {
	ret := make([]T, m.numCols)
	for col := range m.numCols {
		ret[col] = m.At(row, col)
	}
	return ret
}

func (m *ColMajorMatrix[T]) RowSlice(row, colStart, colEnd int) []T {
	ret := make([]T, m.numCols)
	for col := colStart; col < colEnd; col++ {
		ret[col] = m.At(row, col)
	}
	return ret
}

func matrixString[T Number](m Matrix[T]) string {
	numRows, numCols := m.Height(), m.Width()
	rows := make([]string, numRows+2)
	cells := make([]string, numCols)
	maxNumDigitsPerColumn := make([]int, numCols)
	for c := range numCols {
		col := m.Col(c)
		maxNumDigits := 0
		for r := range numRows {
			maxNumDigits = max(maxNumDigits, numDigits(col[r]))
		}
		maxNumDigitsPerColumn[c] = maxNumDigits
	}
	// numSpaces = 3 + sum(maxNumDigits(x) + 1 for each col x)
	cover := strings.Repeat("-", 3+Sum(maxNumDigitsPerColumn)+numCols)
	rows[0] = cover
	rows[len(rows)-1] = cover

	for r := 0; r < numRows; r++ {
		for c, v := range m.Row(r) {
			cells[c] = fmt.Sprint(v) + strings.Repeat(" ", maxNumDigitsPerColumn[c]-numDigits(v))
		}
		row := "| "
		row += strings.Join(cells, " ")
		row += " |"
		rows[r+1] = row
	}
	return strings.Join(rows, "\n")
}

func (m *RowMajorMatrix[T]) String() string {
	return matrixString(m)
}

func (m *ColMajorMatrix[T]) String() string {
	return matrixString(m)
}