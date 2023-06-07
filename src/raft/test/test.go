package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	mu := sync.Mutex{}
	a := 1
	go func() {
		mu.Lock()
		defer mu.Unlock()
		hello(&a)
	}()

	go func() {
		mu.Lock()
		for i := 0; i < 1000; i++ {
			a = 1
		}
		mu.Unlock()
	}()
	time.Sleep(10 * time.Second)
}

func hello(a *int) {
	for i := 0; i < 1000; i++ {
		*a = 1
	}
	fmt.Printf("hello\n")
}
