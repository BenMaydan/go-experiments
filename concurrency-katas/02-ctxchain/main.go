package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
	"strconv"
)

type ctxKey string

const reqIDKey ctxKey = "id"


func handleRequest(ctx context.Context, reqID string) error {
	nctx := context.WithValue(ctx, reqIDKey, reqID)
	res, err := fetchData(nctx)
	fmt.Println(res)
	return err
}

func fetchData(ctx context.Context) ([]int, error) {
	return computeExpensive(ctx)
}

func computeExpensive(ctx context.Context) ([]int, error) {
	iterations := 1000
	res := make([]int, iterations + 2)
	res[0] = 0
	res[1] = 1

	fmt.Printf("Context reqID: %v\n", ctx.Value(reqIDKey))

	for i := range iterations {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		res[i + 2] = res[i] + res[i + 1]
		time.Sleep(time.Millisecond)
	}

	return res, nil
}

func main() {
	userArgs := os.Args[1:]
	timeout := 1000
	var err error = nil
	if len(userArgs) > 0 {
		timeout, err = strconv.Atoi(userArgs[0])
		if err != nil {
			fmt.Println(err)
			return
		}
	}


	// a timeout less than ~2000 milliseconds is guaranteed to error out
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout) * time.Millisecond)
	defer cancel()


	err = handleRequest(ctx, "this is a baddd ;) request")


	if errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("error is context.DeadlineExceeded")
	} else {
		fmt.Printf("error is weird: %v\n", err)
	}
}