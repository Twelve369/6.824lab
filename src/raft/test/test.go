package main

import "fmt"

func main() {
	input := make([]int, 0)
	fmt.Println("Origianl:", input)
	dealData(&input)
	fmt.Println("Output:", input)
}

func dealData(input *[]int) {
	for i := 0; i < 10; i++ {
		*input = append(*input, i)
	}
}
