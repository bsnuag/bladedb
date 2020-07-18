package bladedb

import (
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"runtime"
)

var db *bladeDB = nil
var ServerAddress = ""

func Open(path string) {
	runtime.GOMAXPROCS(100)
	loadConfigs(path)
	setupLogger()

	err := CreateDirectory(db.config.LogDir)
	if err != nil {
		db.logger.Fatal().Err(err).Str("directory ", db.config.LogDir).Msg("Error while creating log directory")
	}

	err = CreateDirectory(db.config.DataDir)
	if err != nil {
		db.logger.Fatal().Err(err).Str("directory ", db.config.LogDir).Msg("Error while creating SST directory")

	}
	PreparePartitionIdsMap()
}

func loadConfigs(configPath string) {
	db = &bladeDB{}
	config := &Config{LogFileMaxLen: DefaultLogFileMaxLen}
	yamlFile, err := ioutil.ReadFile(configPath)
	if os.IsNotExist(err) {
		panic(fmt.Sprintf("could not locate config file: %s", configPath))
	} else if err != nil {
		panic(errors.Wrapf(err, "could not load config file: %s", configPath))
	}
	err = yaml.Unmarshal(yamlFile, config)
	if err != nil {
		panic(errors.Wrapf(err, "could not load config file: %s", configPath))
	}
	if err := validateConfig(config); err != nil {
		panic(errors.Wrapf(err, "could not load config file: %s", configPath))
	}
	db.config = config
	db.logEncoderBuf = make([]byte, uint32(db.config.NoOfPartitions)*LogEncoderPartLen) //this can be moved to other method
	db.walFlushQueue = make(chan bool)
	db.pMap = make(map[int]*PartitionInfo)
	ServerAddress = fmt.Sprintf("0.0.0.0:%d", db.config.ClientListenPort)
}

func validateConfig(config *Config) error {
	//if false == strings.HasSuffix(config.DataDir, "/") {
	//	panic(" must ends with /")
	//}
	return nil
}
