package main

import "fmt"

func main() {
	a := []int{1, 2, 3, 4, 5}
	b := []int{6, 7, 8, 9, 10}
	for i := 0; i < len(b); i++ {
		a = append(a, b[i])
	}
	fmt.Println(a)
}
