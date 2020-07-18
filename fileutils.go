package bladedb

import (
	"os"
)

func CreateDirectory(path string) error {
	pathExists, err := exists(path)
	if err != nil {
		return err
	}
	if !pathExists {
		return os.MkdirAll(path, 0700)
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
