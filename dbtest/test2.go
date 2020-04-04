package main

import (
	"bladedb"
	"fmt"
)

func main() {
	key := "abc:%d"
	value := "abc:%d"
	for i := 0; i < 1; i++ {
		var off uint32 = 0
		ts := bladedb.NanoTime()
		logRec1 := bladedb.LogRecord{1, []byte(fmt.Sprintf(key, i)), []byte(fmt.Sprintf(value, i)), ts}
		encoded := logRec1.Encode()
		logRec2 := bladedb.Decode(&off, encoded)
		fmt.Println(logRec2)
	}
}
