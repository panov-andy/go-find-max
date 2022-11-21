package clickhouse

import (
	"fmt"
	"io"
	"os"
	"runtime"
)

func FileEndLineSeekerByPath(filepath string) ([]int64, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return FileEndLineSeeker(file, int64(runtime.NumCPU()))
}

func FileEndLineSeeker(file *os.File, desiredChunks int64) ([]int64, error) {
	result := make([]int64, 0)
	result = append(result, 0)

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	chunkSize := fileSize / desiredChunks
	currentFileOffset := chunkSize
	for {
		_, err := file.Seek(currentFileOffset, 0)
		if err != nil {
			return nil, err
		}
		readBuff := make([]byte, 8*os.Getpagesize())
		readLen, err := file.Read(readBuff)
		if err != nil {
			if readLen == 0 && err == io.EOF {
				break
			}
			return nil, err
		}

		offsetAfterNewLine := FindOffsetAfterNewLine(readBuff, readLen)
		if offsetAfterNewLine != -1 {
			currentFileOffset = currentFileOffset + offsetAfterNewLine + chunkSize
			result = append(result, currentFileOffset)
		} else {
			currentFileOffset += int64(readLen)
		}
	}

	//hack to make it convenient
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	result = append(result, info.Size()+1)

	return result, nil
}

func FindOffsetAfterNewLine(readBuff []byte, readLen int) int64 {
	wasNewLine := false
	beforeNewLineIndex := -1
	for i := readLen - 1; i >= 0; i-- {
		if readBuff[i] == '\n' || readBuff[i] == '\r' {
			wasNewLine = true
		} else if wasNewLine {
			return int64(beforeNewLineIndex)
		} else {
			beforeNewLineIndex = i
		}
	}
	return -1
}

func FilePartialRead(filepath string, startPosition int64, nonInclusiveEndPosition int64, bytesParser func([]byte, bool)) error {
	fmt.Printf("OPEN FILE: %s\n", filepath)
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	readOffset := startPosition

	endOfFile := false

	for !endOfFile {
		_, err := file.Seek(readOffset, 0)
		if err != nil {
			return err
		}

		pageSize := 8 * os.Getpagesize()
		bufferSize := int64(pageSize)
		//assume that any read will not exceed boundaries
		if readOffset+bufferSize >= nonInclusiveEndPosition {
			bufferSize = nonInclusiveEndPosition - readOffset - int64(1)
			endOfFile = true
		}
		buff := make([]byte, bufferSize)

		_, err = file.Read(buff)
		if err != nil {
			if err == io.EOF {
				endOfFile = true
				bytesParser(buff, endOfFile)
				return nil
			}
			return err
		}
		readOffset += int64(len(buff))
		bytesParser(buff, endOfFile)
	}
	return nil
}
