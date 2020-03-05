package bladedb

import (
	"os"
)

func createDirectory(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}
