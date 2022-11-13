package clickhouse

import (
	"bytes"
	"io"
	"os"
	"sort"
)

type Cortege struct {
	Url  string
	Rate int
}

type Collector struct {
	maxSize  int
	corteges []Cortege
}

func (c *Collector) process(cortege Cortege) {
	arrSize := len(c.corteges)

	if arrSize < c.maxSize {
		c.corteges = append(c.corteges, cortege)
	} else {
		if c.corteges[arrSize-1].Rate < cortege.Rate {
			c.corteges[arrSize-1] = cortege
		}
	}

	sortByRate(c.corteges)
}

func sortByRate(corteges []Cortege) {
	sort.SliceStable(corteges, func(i, j int) bool {
		return corteges[i].Rate > corteges[j].Rate
	})
}

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
		buff := make([]byte, 17)
		readBytes, err := file.Read(buff)
		if err != nil {
			if readBytes == 0 && err == io.EOF {
				parser.finish()
				break
			}
			return err
		}
		parser.submitChunk(buff, readBytes)
	}
	return nil
}

type Parser struct {
	buffer bytes.Buffer
}

func (p *Parser) submitChunk(bytes []byte, readBytes int) {
	p.buffer.WriteString(string(bytes[:readBytes]))
}

func (p *Parser) content() string {
	return p.buffer.String()
}

func (p *Parser) finish() {

}
