package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)


type Result string

func generate(ctx context.Context, n int) <-chan int {
	sendCh := make(chan int)

	go func() {
		defer close(sendCh)
		for i := range n {
			select {
			case <-ctx.Done():
				return
			case sendCh <- (i + 1):
			}
		}
	}()

	return sendCh
}

func isPrime(ctx context.Context, v int) (bool, error) {
	if v == 1 { return false, nil }
	if v == 2 { return true, nil }
	for div := int(math.Sqrt(float64(v))); div > 1; div-- {
		if ctx.Err() != nil { return false, ctx.Err() }
		if v % div == 0 { return false, nil }
	}
	return true, nil
}

func worker(ctx context.Context, in <-chan int) <-chan Result {
	results := make(chan Result)

	go func() {
		defer close(results)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok { return }
				res, _ := isPrime(ctx, v)
				select {
				case <-ctx.Done():
					return
				case results <- Result(fmt.Sprintf("The number %v has a prime check of %v", v, res)):
				}
			}
		}
	}()

	return results
}

func merge[T any](ctx context.Context, chans ...<-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
		wg.Add(len(chans))

		for _, ch := range chans {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case v, ok := <-ch:
						if !ok { return }
						select {
						case <-ctx.Done():
							return
						case out <- v:
						}
					}
				}
			}()
		}

		wg.Wait()
	}()

	return out
}

func main() {
	n := 100000000
	numWorkers := 32
	printResults := false
	var err error = nil

	if len(os.Args) == 3 {
		n, err = strconv.Atoi(os.Args[1])
		if err != nil { fmt.Println(err); return }

		numWorkers, err = strconv.Atoi(os.Args[2])
		if err != nil { fmt.Println(err); return }

		fmt.Printf("N: %v, Num Workers: %v\n", n, numWorkers)
	}
	if len(os.Args) == 4 {
		printResults = true
	}

	dctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := generate(dctx, n)

	chans := make([]<-chan Result, numWorkers)
	for i := range numWorkers {
		chans[i] = worker(dctx, generator)
	}

	merged := merge(dctx, chans...)
	results := make([]Result, n)
	i := 0
	start := time.Now()
	for v := range merged {
		results[i] = v
		i++
	}
	fmt.Printf("Elapsed: %v\n", time.Since(start))

	if printResults {
		for _, result := range results {
			fmt.Println(result)
		}
	}
}