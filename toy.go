package main

import "fmt"

func main() {
	s := []int{1, 2, 3, 4, 5}
	S := s[:0]
	S[4] = 10
	fmt.Println(s)
}