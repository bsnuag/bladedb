package main

import (
	"container/heap"
	"fmt"
)

type HeapEle struct {
	val  int
	time int
}

// An IntHeap is a min-heap of ints.
type IntHeap []HeapEle

func (h IntHeap) Len() int { return len(h) }
func (h IntHeap) Less(i, j int) bool {
	if h[i].val == h[j].val {
		return h[i].time > h[j].time
	}
	return h[i].val < h[j].val
}

func (h IntHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapEle))
}

func (h *IntHeap) Pop() interface{} {
	//fmt.Println(h)
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *IntHeap) Top() interface{} {
	return (*h)[0]
}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
func main() {
	h := &IntHeap{HeapEle{2, 4}, HeapEle{1, 3}, HeapEle{5, 7}}
	heap.Init(h)

	heap.Push(h, HeapEle{1, 6})
	heap.Push(h, HeapEle{1, 8})
	fmt.Println(h)

	//fmt.Printf("minimum: %d\n", (*h)[0])
	//i := 10
	for h.Len() > 0 {
		fmt.Println(h.Top())
		fmt.Println(heap.Pop(h))
	}
	fmt.Printf("done...")
	//for h.Len() > 0 {
	//	fmt.Printf("%d ", heap.Pop(h))
	//	heap.Push(h, i)
	//	i++
	//	time.Sleep(1000000)
	//}
}
