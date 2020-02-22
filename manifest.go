package bladedb

import (
	"fmt"
	"github.com/niubaoshu/gotiny"
	"io"
	"os"
	"sync"
)

var manifestFileName = "data/MANIFEST"
var manifestRecLen = 50

type ManifestFile struct {
	file     *os.File
	manifest *Manifest
	mutex    sync.Mutex
}

type Manifest struct {
	sstManifest map[int]ManifestRecs //partId -> ManifestRec
	logManifest map[int]ManifestRecs //TODO - implement - remove logstatwriter
}

type ManifestRecs struct {
	manifestRecs map[uint32]ManifestRec
}
type ManifestRec struct {
	// 8 + 32 + 8 + 1 + 1 = 50 byte
	partitionId int
	seqNum      uint32
	levelNum    int
	fop         byte //create - 0 , 1 - delete
	fileType    byte //0 - log file , 1 - sst file
}

var manifestFile *ManifestFile = nil

func initManifest() (error) {
	file, err := os.OpenFile(manifestFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("Error while opening or creating manifest file")
		return err
	}
	manifestFile = &ManifestFile{
		file:     file,
		manifest: nil,
	}
	err = replay()
	if err != nil {
		panic("Error while reading manifest file")
		return err
	}
	return nil
}

//https://blog.cloudflare.com/recycling-memory-buffers-in-go/
func write(manifestArr []ManifestRec) error { //TODO - when grows to a threshold - re-write info
	manifestFile.mutex.Lock()
	defer manifestFile.mutex.Unlock()

	byteBuf := make([]byte, manifestRecLen*len(manifestArr))
	var byteIndex = 0
	for _, manifest := range manifestArr {
		marshal := gotiny.Marshal(&manifest)
		copy(byteBuf[byteIndex:byteIndex+manifestRecLen], marshal)
		byteIndex += manifestRecLen
	}
	if _, err := manifestFile.file.Write(byteBuf); err != nil {
		return err
	}
	if err := manifestFile.file.Sync(); err != nil {
		return err
	}
	return nil
}

func replay() error { //TODO - TEST is failing
	manifestFile.file.Seek(0, 0)
	manifest := Manifest{
		sstManifest: make(map[int]ManifestRecs),
		logManifest: make(map[int]ManifestRecs),
	}
	for partitionId := 0; partitionId < defaultConstants.noOfPartitions; partitionId++ {
		manifest.sstManifest[partitionId] = ManifestRecs{
			manifestRecs: make(map[uint32]ManifestRec),
		}
		manifest.logManifest[partitionId] = ManifestRecs{
			manifestRecs: make(map[uint32]ManifestRec),
		}
	}
	recCount := 0
	for {
		buf := make([]byte, manifestRecLen)
		_, err := manifestFile.file.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("manifest file read complete")
				break
			} else {
				return err
			}
		}
		mRec := ManifestRec{}
		gotiny.Unmarshal(buf, &mRec)
		recCount++
		if mRec.fileType == 1 {
			manifest.sstManifest[mRec.partitionId].manifestRecs[mRec.seqNum] = mRec
		} else if mRec.fileType == 0 {
			manifest.logManifest[mRec.partitionId].manifestRecs[mRec.seqNum] = mRec
		} else {
			//error
		}
	}
	if recCount > 0 {
		manifestFile.manifest = &manifest
	}
	return nil
}
