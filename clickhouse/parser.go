package clickhouse

import (
	"bytes"
	"strconv"
	"sync"
)

func NewParser(collector *Collector) *Parser {
	url := bytes.Buffer{}
	number := bytes.Buffer{}

	parser := &Parser{
		collector: collector,
		url:       url,
		number:    number,
		newLine:   false,
		Wg:        sync.WaitGroup{},
	}
	parser.target = &parser.url

	return parser
}

type Parser struct {
	collector *Collector

	url     bytes.Buffer
	number  bytes.Buffer
	target  *bytes.Buffer
	newLine bool

	Wg sync.WaitGroup
}

func (p *Parser) SubmitChunk(bytes []byte, readBytes int) {
	for i := 0; i < readBytes; i++ {
		if bytes[i] == ' ' {
			p.target = &p.number
		} else if bytes[i] == '\n' || bytes[i] == '\r' {
			p.newLine = true
		} else {
			if p.newLine {
				p.ParseCortege()

				p.target = &p.url
				p.url.Truncate(0)
				p.number.Truncate(0)
				p.newLine = false
			}

			p.target.WriteString(string(bytes[i]))
		}
	}
}

func (p *Parser) ParseCortege() {
	if p.number.Len() == 0 {
		panic("empty number for url:" + p.url.String())
	}
	value, err := strconv.Atoi(p.number.String())
	if err != nil {
		panic("parse a value: " + err.Error())
	}
	cortege := Cortege{Url: p.url.String(), Rate: value}
	p.collector.process(&cortege)

}
