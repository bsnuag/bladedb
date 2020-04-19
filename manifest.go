package bladedb

import (
	"github.com/niubaoshu/gotiny"
	"io"
	"os"
)

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

func initManifest() (error, *Manifest) {
	file, err := os.OpenFile(db.config.DataDir+ManifestFileFmt, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err, nil
	}
	db.manifestFile = file
	return replayManifest()
}

func closeManifest() error {
	if db.manifestFile == nil {
		return nil
	}
	if err := db.manifestFile.Close(); err != nil { //do we need to sync or close is enough?
		return err
	}
	return nil
}

//https://blog.cloudflare.com/recycling-memory-buffers-in-go/
func writeManifest(manifestArr []ManifestRec) error { //TODO - when grows to a threshold - re-writeManifest info
	db.manifestLock.Lock()
	defer db.manifestLock.Unlock()

	byteBuf := make([]byte, ManifestRecLen*len(manifestArr))
	var byteIndex = 0
	for _, manifest := range manifestArr {
		marshal := gotiny.Marshal(&manifest)
		copy(byteBuf[byteIndex:byteIndex+ManifestRecLen], marshal)
		byteIndex += ManifestRecLen
	}
	if _, err := db.manifestFile.Write(byteBuf); err != nil {
		return err
	}
	if err := db.manifestFile.Sync(); err != nil { //test and remove this - already provided while creating file
		return err
	}
	return nil
}

func replayManifest() (error, *Manifest) {
	db.manifestFile.Seek(0, 0)
	manifest := Manifest{
		sstManifest: make(map[int]ManifestRecs),
		logManifest: make(map[int]ManifestRecs),
	}
	for partitionId := 0; partitionId < db.config.NoOfPartitions; partitionId++ {
		manifest.sstManifest[partitionId] = ManifestRecs{
			manifestRecs: make(map[uint32]ManifestRec),
		}
		manifest.logManifest[partitionId] = ManifestRecs{
			manifestRecs: make(map[uint32]ManifestRec),
		}
	}
	recCount := 0
	for {
		buf := make([]byte, ManifestRecLen)
		_, err := db.manifestFile.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err, nil
			}
		}
		mRec := ManifestRec{}
		gotiny.Unmarshal(buf, &mRec)
		recCount++
		if mRec.fileType == DataFileType {
			manifest.sstManifest[mRec.partitionId].manifestRecs[mRec.seqNum] = mRec
		} else if mRec.fileType == LogFileType {
			manifest.logManifest[mRec.partitionId].manifestRecs[mRec.seqNum] = mRec
		} else {
			db.logger.Error().Msg("unsupported ty manifest rec type, supports 0 & 1")
		}
	}
	if recCount > 0 {
		return nil, &manifest
	}
	return nil, nil
}
