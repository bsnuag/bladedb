package bladedb

import (
	"fmt"
	"github.com/niubaoshu/gotiny"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
)

var ManifestFileName = "data/MANIFEST"
var manifestRecLen = 50

type ManifestFile struct {
	file     *os.File
	manifest *Manifest
	mutex    sync.Mutex
}

type Manifest struct {
	sstManifest map[int]ManifestRecs //partId -> ManifestRec
	logManifest map[int]ManifestRecs
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

func initManifest() error {
	file, err := os.OpenFile(ManifestFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "Error while opening or creating manifest file: %s", ManifestFileName)
	}
	manifestFile = &ManifestFile{
		file:     file,
		manifest: nil,
	}
	err = replay()
	if err != nil {
		return errors.Wrapf(err, "Error while replaying manifest file: %s", ManifestFileName)
	}
	return nil
}

func closeManifest() error {
	if manifestFile == nil || manifestFile.file == nil {
		manifestFile = nil
		return nil
	}
	if err := manifestFile.file.Close(); err != nil { //do we need to sync or close is enough?
		return err
	}
	manifestFile = nil
	return nil
}

//https://blog.cloudflare.com/recycling-memory-buffers-in-go/
func writeManifest(manifestArr []ManifestRec) error { //TODO - when grows to a threshold - re-writeManifest info
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
	if err := manifestFile.file.Sync(); err != nil { //test and remove this - already provided while creating file
		return err
	}
	return nil
}

func replay() error {
	manifestFile.file.Seek(0, 0)
	manifest := Manifest{
		sstManifest: make(map[int]ManifestRecs),
		logManifest: make(map[int]ManifestRecs),
	}
	for partitionId := 0; partitionId < DefaultConstants.noOfPartitions; partitionId++ {
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
