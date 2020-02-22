package main

import (
	"bytes"
	"fmt"
)

func main() {
	/*one := make([]byte, 4)
	two := make([]byte, 5)
	one[0] = 0x00
	one[1] = 0x01
	two[0] = 0x02
	two[1] = 0x03
	fmt.Println(one)
	fmt.Println(two)
	var buf bytes.Buffer
	buf.Reset()

	buf.Write(one)
	buf.Write(two)
	fmt.Println(buf.Bytes())
	fmt.Println("-----------")

	s1 := &Student{
		name: "bishnu",
		roll: 1,
		city: "BLR",
	}
	marshal := gotiny.Marshal(s1)

	fmt.Println(marshal)
	fmt.Println(len(marshal))

	var b = make([]byte,20)
	copy(b[2:10], marshal)

	fmt.Println(b)
	fmt.Println(len(b))
	s2:= &Student{}
	unmarshal := gotiny.Unmarshal(b, s2)

	fmt.Println(unmarshal)
	fmt.Println(s2)*/
	buff := bytes.NewBuffer(make([]byte,50))
	one := make([]byte,6)
	two := make([]byte,6)
	one[0] = 1
	one[1] = 2
	one[2] = 3
	one[3] = 4
	one[4] = 5
	one[5] = 6
	buff.Read(two)
	buff.Write(one)
	fmt.Println(one)
	fmt.Println(buff)
	fmt.Println(buff.Bytes())
	buff.Reset()
	fmt.Println(buff)
	fmt.Println(buff.Bytes())
	fmt.Println("--")
	fmt.Println(two)
}

type Student struct {
	name string
	roll int
	city string
}
