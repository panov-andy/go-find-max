package clickhouse

import (
	"io"
	"os"
)

func ReadFile(filepath string, parser *Parser) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	for {
		_, err := file.Seek(0, 1)
		if err != nil {
			return err
		}
		buff := make([]byte, 8*os.Getpagesize())
		readBytes, err := file.Read(buff)
		if err != nil {
			if readBytes == 0 && err == io.EOF {
				parser.ParseCortege()
				break
			}
			return err
		}
		parser.SubmitChunk(buff, readBytes)
	}
	return nil
}
