package bladedb

import (
	"errors"
	"github.com/rs/zerolog"
	"os"
	"sync"
)

var (
	EmptyFile = errors.New("EmptyFile")
)

const (
	WriteReq             byte   = 0
	DelReq               byte   = 1
	KeyMaxLen            uint32 = 1<<16 - 1
	ValueMaxLen          uint32 = 1<<16 - 1
	fDelete              byte   = 1
	fCreate              byte   = 0
	LogFileType          byte   = 0
	DataFileType         byte   = 1
	MaxLevel             int    = 6
	MaxDataFileSz        uint32 = 64e+6
	MaxSSTCompact        int    = 32
	MinSSTCompact        int    = 16
	LogFileFmt                  = "log_%d_%d.log"
	DataFileFmt                 = "sst_%d_%d.sst"
	DefaultLogFileMaxLen uint32 = 32e+6
	LogEncoderBufMetaLen uint32 = 1<<8 - 1                                       //1 byte(recType) + 16 bytes(ts)
	LogEncoderPartLen    uint32 = LogEncoderBufMetaLen + KeyMaxLen + ValueMaxLen //is shared among partitions, each works within a range of offset-offset+LogEncoderPartLen. offset = partId * LogEncoderPartLen
	ManifestRecLen              = 50
	ManifestFileFmt             = "MANIFEST" //has to be handled like log & sst
	SSTEncoderBufMetaLen uint32 = 1<<8 - 1   //1 byte(recType) + 16 bytes(ts)
	SSTBufLen            uint32 = SSTEncoderBufMetaLen + KeyMaxLen + KeyMaxLen
)

var LevelCompactInf = map[int]uint32{
	0: 4,
	1: 100,
	2: 1000,
	3: 10000,
	4: 100000,
	5: 1000000,
	6: 0, //infinite
}

type bladeDB struct {
	config                   *Config
	pMap                     map[int]*PartitionInfo
	logger                   zerolog.Logger
	memFlushTaskQueue        chan *InactiveLogDetails
	activeMemFlushSubscriber sync.WaitGroup
	memFlushActive           int32
	compactTaskQueue         chan *CompactInfo //TODO - change channel type to compactTask with pId and thisLevel
	compactSubscriber        sync.WaitGroup
	compactActive            int32 //0- false, 1 - true
	walFlushQueue            chan bool
	logEncoderBuf            []byte // - is shared among partitions, each works within a range of offset-offset+LogEncoderPartLen. offset = partId * LogEncoderPartLen
	manifestFile             *os.File
	manifestLock             sync.Mutex
}

type Config struct {
	LogFileMaxLen       uint32
	LogDir              string `yaml:"log-dir"`
	DataDir             string `yaml:"data-dir"`
	WalFlushPeriodInSec int    `yaml:"log-flush-interval"`
	NoOfPartitions      int    `yaml:"partitions"`
	CompactWorker       int    `yaml:"compact-worker"`
	MemFlushWorker      int    `yaml:"memflush-worker"`
	ClientListenPort    int    `yaml:"client-listen-port"`
	LoggerLevel         string `yaml:"log-level"` //test it
}
