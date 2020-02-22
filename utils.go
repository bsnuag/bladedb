package bladedb

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
)
//expected file format - wal_seqNum_partitionId
//returns seqNum+1 if file with seqNum 0 or greater present else 0

//TODO - This has to replace, nextSeq num must be atomic - cannot read directory each time
func GetFileNextSeqNum(partitionId int, fileDir string, filePattern string) (uint32, error) {
	files, err := ioutil.ReadDir(fileDir)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error while getting next seq num for " +
			"partitionId: %d, fileDir: %s, filePattern: %s, error: %v", partitionId, fileDir, filePattern, err))
		log.Fatal(err)
		return 0, err
	}
	pattern:=fmt.Sprintf("[0-9]+_%d", partitionId)
	compiledPattern := regexp.MustCompile(pattern)
	var seqNum uint32 = 0

	for _ , f := range files {
		matched, _ := regexp.Match(filePattern, []byte(f.Name()))
		if matched {
			matchedStringArr := strings.Split(compiledPattern.FindString(f.Name()),"_")
			tempSeqNo, err := strconv.Atoi(matchedStringArr[0])
			if err!=nil{
				panic(fmt.Sprintf("Error while getting next seq num for " +
					"partitionId: %d, fileDir: %s, filePattern: %s, error: %v", partitionId, fileDir, filePattern, err))
			}
			if seqNum < uint32(tempSeqNo) {
				seqNum = uint32(tempSeqNo)
			}
		}
	}
	return seqNum+1, nil
}

func GetHash(input []byte) (string, error) {
	h := sha256.New()
	_, err := h.Write(input)
	if err!=nil {
		panic(err)
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}






