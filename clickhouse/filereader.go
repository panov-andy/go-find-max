package clickhouse

import (
	"io"
	"os"
)

func FileEndLineSeeker(file *os.File, desiredChunks int64) ([]int, error) {
	result := make([]int, 0)
	result = append(result, 0)

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	desiredOffset := fileSize / desiredChunks
	offset := desiredOffset
	for {
		_, err := file.Seek(offset, 0)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 8*os.Getpagesize())
		readBytes, err := file.Read(buff)
		if err != nil {
			if readBytes == 0 && err == io.EOF {
				parser.parseCortege()
				break
			}
			return err
		}
		parser.submitChunk(buff, readBytes)
	}

	return result, nil
}

func FilePartialReadNewLine(filepath string, startPosition int, length int, closure func([]byte, int, bool)) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := startPosition

	for {
		_, err := file.Seek(0, 1)
		if err != nil {
			return err
		}
		buff := make([]byte, 8*os.Getpagesize())
		readBytes, err := file.Read(buff)
		if err != nil {
			if readBytes == 0 && err == io.EOF {
				parser.parseCortege()
				break
			}
			return err
		}
		parser.submitChunk(buff, readBytes)
	}
	return nil

}
