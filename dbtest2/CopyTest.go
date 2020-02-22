package main

import "fmt"

func main() {
	t:=Test{

	}
	fmt.Println(t.name)
}

type Test struct {
	name string
	hashmap map[uint32]int64
}

