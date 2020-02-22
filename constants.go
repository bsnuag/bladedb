package bladedb

import "time"

//const WriteReq byte = 0
//const DeleteReq byte = 1
//
//const LogStatRecLen = 4     // bytes
//const LogFileMaxLen = 32e+6 //32MB
//const LogRecLen = 5
//
//const MarkLogFileDelete = 1
//const MarkLogFileNew = 0
//
//const LogFileStartOffset = 0
//const walFlushPeriodInSec = 10
//
//const MaxLevel = 2 //max SST levels (0,1,2)
//
//const noOfPartitions = 1 // should be handled properly when adding multiple nodes
//
//const MaxSSTSize = 64e+6
//
//var SSTDir = "data/dbstore/"

type Constants struct {
	writeReq  byte
	deleteReq byte

	logStatRecLen int
	logFileMaxLen uint32
	logRecLen     uint32

	fileDelete byte
	fileCreate byte

	logFileType byte
	sstFileType byte

	logFileStartOffset  uint32
	walFlushPeriodInSec time.Duration

	maxLevel int //max SST levels

	noOfPartitions int // should be handled properly when adding multiple nodes

	maxSSTSize uint32

	SSTDir        string
	maxSSTCompact int
	minSSTCompact int
	levelMaxSST   map[int]uint32
}

var defaultConstants = Constants{
	writeReq:            0,
	deleteReq:           1,
	logStatRecLen:       4,
	logFileMaxLen:       32e+6,
	logRecLen:           5,
	fileDelete:          1,
	fileCreate:          0,
	logFileType:         0,
	sstFileType:         1,
	logFileStartOffset:  0,
	walFlushPeriodInSec: 10,
	maxLevel:            3,
	noOfPartitions:      1,
	maxSSTSize:          64e+6, //64 MB
	SSTDir:              "data/dbstore/",
	maxSSTCompact:       32, //soft value
	minSSTCompact:       16, //TODO - can we make it % based ..? 10 % of total SST ??
	levelMaxSST: map[int]uint32{
		0: 4,
		1: 100,
		2: 1000,
		3: 0, //infinite
	},
}
