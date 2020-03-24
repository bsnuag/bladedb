package bladedb

import (
	"os"
)

func CreateDirectory(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}
