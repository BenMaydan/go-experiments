package workerpool

import (
	"errors"
	"sync"
	"iter"
)

// noCopy is a zero-byte struct used to make structs uncopyable.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// queue should be uncopyable because it contains a mutex
type Queue[T any] struct {
	sync.Mutex
	noCopy noCopy
	elements []T
}

func (queue *Queue[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range queue.elements {
			// Pass the item to the loop. Stop if yield returns false.
			if !yield(item) {
				return
			}
		}
	}
}

func (queue *Queue[T]) Add(elem T) {
	queue.Lock()
	defer queue.Unlock()
	// I cannot append since Pop takes the last element
	// Need to add to beginning of array
	queue.elements = append([]T{elem}, queue.elements...)
}

func (queue *Queue[T]) Peek() (elem T, err error) {
	queue.Lock()
	defer queue.Unlock()

	// handles empty case
	if len(queue.elements) == 0 {
		// is this panic-able?
		// I don't think so because it signals to the dispatcher to hold in the select statement
		err = errors.New("cannot peek empty queue")
		return
	}

	lastIdx := len(queue.elements) - 1
	elem = queue.elements[lastIdx]
	return
}

func (queue *Queue[T]) Pop() (elem T, err error) {
	queue.Lock()
	defer queue.Unlock()

	// handles empty case
	if len(queue.elements) == 0 {
		// is this panic-able?
		// I don't think so because it signals to the dispatcher to hold in the select statement
		err = errors.New("cannot pop empty queue")
		return
	}

	var zero T
	lastIdx := len(queue.elements) - 1

	elem = queue.elements[lastIdx]
	queue.elements[lastIdx] = zero
	queue.elements = queue.elements[:lastIdx]

	return
}

func (queue *Queue[T]) Empty() bool {
	queue.Lock()
	defer queue.Unlock()

	return len(queue.elements) == 0
}

func (queue *Queue[T]) EmptyOut() {
	queue.Lock()
	defer queue.Unlock()

	if len(queue.elements) == 0 { return }
	clear(queue.elements)
}

func (queue *Queue[T]) Size() int {
	queue.Lock()
	defer queue.Unlock()

	return len(queue.elements)
}
