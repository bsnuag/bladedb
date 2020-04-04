package bladedb

import "time"

type Constants struct {
	writeReq  byte
	deleteReq byte

	logFileMaxLen   uint32
	logRecHeaderLen int

	fileDelete byte
	fileCreate byte

	logFileType byte
	sstFileType byte

	logFileStartOffset  uint32
	walFlushPeriodInSec time.Duration

	maxLevel int //max SST levels

	noOfPartitions int // should be handled properly when adding multiple nodes

	maxSSTSize uint32

	maxSSTCompact  int
	minSSTCompact  int
	levelMaxSST    map[int]uint32
	compactWorker  int
	compactActive  int32
	memFlushWorker int

	//network
	ClientListenPort int
}

var DefaultConstants = Constants{
	writeReq:            0,
	deleteReq:           1,
	logFileMaxLen:       32e+6,
	logRecHeaderLen:     6,
	fileDelete:          1,
	fileCreate:          0,
	logFileType:         0,
	sstFileType:         1,
	logFileStartOffset:  0,
	walFlushPeriodInSec: 10,
	maxLevel:            6,
	noOfPartitions:      100,
	maxSSTSize:          64e+6, //64 MB
	maxSSTCompact:       32,    //soft value
	minSSTCompact:       16,    //TODO - can we make it % based ..? 10 % of total SST ??
	levelMaxSST: map[int]uint32{
		0: 4,
		1: 100,
		2: 1000,
		3: 10000,
		4: 100000,
		5: 1000000,
		6: 0, //infinite
	},
	compactWorker:  8,
	compactActive:  1, //1=true, 0 = false
	memFlushWorker: 8,

	ClientListenPort: 9099,
}
