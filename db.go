package bladedb

import (
	"github.com/pkg/errors"
	"runtime"
)

func Open() error {
	runtime.GOMAXPROCS(100)
	err := CreateDirectory(LogDir)
	if err != nil {
		return errors.Wrapf(err, "Error while creating directory: %s, error: %v", LogDir, err)
	}

	err = CreateDirectory(SSTDir)
	if err != nil {
		return errors.Wrapf(err, "Error while creating directory: %s, error: %v", SSTDir, err)

	}

	if err = PreparePartitionIdsMap(); err != nil {
		return err
	}
	return nil
}
