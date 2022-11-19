package clickhouse

import (
	"io"
	"os"
)

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
			currentFileOffset += offsetAfterNewLine
			result = append(result, currentFileOffset)
		} else {
			currentFileOffset += int64(readLen)
		}
	}

	return result, nil
}

func FindOffsetAfterNewLine(readBuff []byte, readLen int) int64 {
	newLine := false
	for i := 0; i < readLen; i++ {
		if readBuff[i] == '\n' || readBuff[i] == '\r' {
			newLine = true
		} else if newLine {
			return int64(i)
		}
	}
	return -1
}

//func FilePartialReadNewLine(filepath string, startPosition int, length int, closure func([]byte, int, bool)) error {
//	file, err := os.Open(filepath)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	offset := startPosition
//
//	for {
//		_, err := file.Seek(0, 1)
//		if err != nil {
//			return err
//		}
//		buff := make([]byte, 8*os.Getpagesize())
//		readBytes, err := file.Read(buff)
//		if err != nil {
//			if readBytes == 0 && err == io.EOF {
//				parser.parseCortege()
//				break
//			}
//			return err
//		}
//		parser.submitChunk(buff, readBytes)
//	}
//	return nil
//
//}
