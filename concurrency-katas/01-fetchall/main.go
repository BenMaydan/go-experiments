package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
)


func fetchSource(ctx context.Context, id int, failRate float64) (string, error) {
	count := rand.Intn(10) + 1
	iterations := 0

	defer func() {
		if iterations == count {
			log.Printf("source %d completed all iterations", id)
		} else {
			log.Printf("source %d only completed %d iterations before returning", id, iterations)
		}
	}()

	for range count {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		iterations++
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	if rand.Float64() < failRate {
		return "", fmt.Errorf("id %v errored out", id)
	} else {
		return fmt.Sprintf("ID: %v", id), nil
	}
}

func fetchAll(ctx context.Context, sourceIDs []int, failRate float64) ([]string, error) {
	g, gctx := errgroup.WithContext(ctx)
	results := make([]string, len(sourceIDs))

	for i, sourceID := range sourceIDs {
		g.Go(func() error {
			res, err := fetchSource(gctx, sourceID, failRate)
			if err == nil { results[i] = res }
			return err
		})
	}

	err := g.Wait()
	if err == nil {
		return results, nil
	}
	return nil, err
}

func main() {
	n := 64
	sourceIDs := make([]int, n)
	for i := range n {
		sourceIDs[i] = i
	}

	results, err := fetchAll(context.Background(), sourceIDs, 0.1)

	fmt.Printf("error: %v\n", err)
	fmt.Printf("results: %v", results)
}