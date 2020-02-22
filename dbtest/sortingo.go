package main

import (
	"fmt"
	"sync"
)

func main() {
	st1 := St{name:"string"}
	fmt.Println(st1.name)
	fmt.Println(st1==St{})

}

type St struct {
	name string
	age int
}

func work(chans chan int, mutex *sync.Mutex, wg *sync.WaitGroup, maps map[int]string) {
	//var tMap = make(map[int]string)
	for i := range chans {
		val := maps[i]
		mutex.Lock()
		if i == 3 {
			fmt.Println(val)
			wg.Done()
			continue
		}
		fmt.Println(val)
		fmt.Println(i)
		wg.Done()
		mutex.Unlock()
	}
}
