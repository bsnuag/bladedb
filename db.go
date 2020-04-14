package bladedb

import (
	"runtime"
)

func Open() {
	runtime.GOMAXPROCS(100)
	setupLogger()

	err := CreateDirectory(LogDir)
	if err != nil {
		defaultLogger.Fatal().Err(err).Str("directory ", LogDir).Msg("Error while creating log directory")
	}

	err = CreateDirectory(SSTDir)
	if err != nil {
		defaultLogger.Fatal().Err(err).Str("directory ", SSTDir).Msg("Error while creating SST directory")

	}
	PreparePartitionIdsMap()
}
