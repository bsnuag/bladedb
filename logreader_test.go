package bladedb

import (
	"fmt"
	"testing"
	"time"
)

func TestLoadLogFile(t *testing.T)  {
	fmt.Println(time.Now().Nanosecond())
	fmt.Println(time.Now().UnixNano())
	fmt.Println(time.Now().Unix())

	//LogDir = "/var/folders/j0/rfrc2z7n2k1b9x8ky33_zgd40000gn/T/dbTestLog244374058"
	//partitionInfo, _ := NewPartition(1)
	//inactiveLogDetails, _ := partitionInfo.loadLogFile(1)
	//fmt.Println(inactiveLogDetails)
}

//TODO - complete this
