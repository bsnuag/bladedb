package bladedb

var logFileDir = "data/commitlog/"
var logFileFormat = "log_[0-9]+_%d\\.log" //log_seqNum_partitionId

/*//get latest seqnum for wal file for the input partition
//used once while booting up
func GetWalSeqNum(partitionId int) (uint32, error) {
	filePattern := fmt.Sprintf(logFileFormat, partitionId)
	files, err := ioutil.ReadDir(logFileDir)

	if err != nil {
		fmt.Println(fmt.Sprintf("Error while getting next seq num for "+
			"partitionId: %d, fileDir: %s, filePattern: %s, error: %v", partitionId, logFileDir, filePattern, err))
		log.Fatal(err)
		return 0, err
	}
	pattern := fmt.Sprintf("[0-9]+_%d", partitionId)
	compiledPattern := regexp.MustCompile(pattern)
	var seqNum uint32 = 0

	for _, f := range files {
		matched, _ := regexp.Match(filePattern, []byte(f.Name()))
		if matched {
			matchedStringArr := strings.Split(compiledPattern.FindString(f.Name()), "_")
			tempSeqNo, err := strconv.Atoi(matchedStringArr[0])
			if err != nil {
				panic(fmt.Sprintf("Error while getting next seq num for "+
					"partitionId: %d, fileDir: %s, filePattern: %s, error: %v", partitionId, logFileDir, filePattern, err))
			}
			if seqNum < uint32(tempSeqNo) {
				seqNum = uint32(tempSeqNo)
			}
		}
	}
	return seqNum, nil
}*/

/*//get any unclosed wal file name
func GetUnClosedWalFile(partId int) ([]string, error) {
	var unclosedWalFileRecs = make(map[string]*CheckpointRecord)
	var unclosedFileNames = make([]string, 0, 10)

	if len(unclosedWalFileRecs) == 0 {
		if err := loadUnClosedWalFile(unclosedWalFileRecs, partId); err != nil {
			fmt.Printf(fmt.Sprintf("Error while loading closed wal files from log stat file, "+
				"couldn't get unclosed file for partition: %d, error: %v", partId, err))
			return unclosedFileNames, err
		}
	}

	for key, value := range unclosedWalFileRecs {
		if value.partId == partId {
			unclosedFileNames = append(unclosedFileNames, key)
		}
	}

	fmt.Println(fmt.Sprintf("Found %d unclosed wal files for partitionId: %d, fileNames: %v",
		len(unclosedFileNames), partId, unclosedFileNames))

	return unclosedFileNames, nil
}

func loadUnClosedWalFile(unclosedWalFileRecs map[string]*CheckpointRecord, partId int) error {
	statFile, err := os.OpenFile(fmt.Sprintf(statsFileBaseName, partId), os.O_RDWR, 0644)
	if err != nil {
		panic("Error while opening wal stats file, fileName: " + statsFileBaseName)
		return err
	}

	stats, err := statFile.Stat()
	if err != nil {
		panic("Error while getting stats fot wal meta file")
		return err
	}

	var readOffset int64 = 0

	for readOffset < stats.Size() {

		var headerBuf = make([]byte, defaultConstants.logStatRecLen)
		statFile.Read(headerBuf[0:defaultConstants.logStatRecLen])
		var recLength = binary.LittleEndian.Uint32(headerBuf[:])

		var recBuf = make([]byte, recLength)
		statFile.Read(recBuf[:])

		var checkpointRec = CheckpointRecord{}
		gotiny.Unmarshal(recBuf[:], &checkpointRec)

		//delete if file name already exists in map, all closed file will have exactly 2 entry in log stat file
		if _, ok := unclosedWalFileRecs[checkpointRec.logFileName]; !ok {
			unclosedWalFileRecs[checkpointRec.logFileName] = &checkpointRec
		} else {
			delete(unclosedWalFileRecs, checkpointRec.logFileName)
		}

		readOffset += int64(defaultConstants.logStatRecLen) + int64(recLength)
	}
	return nil
}*/
