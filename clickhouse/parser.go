package clickhouse

import (
	"bytes"
	"strconv"
)

func NewParser(collector *Collector) *Parser {
	url := bytes.Buffer{}
	number := bytes.Buffer{}

	parser := &Parser{
		collector: collector,
		url:       url,
		number:    number,
		newLine:   false,
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
}

func (p *Parser) submitChunk(bytes []byte, readBytes int) {
	for i := 0; i < readBytes; i++ {
		if bytes[i] == ' ' {
			p.target = &p.number
		} else if bytes[i] == '\n' || bytes[i] == '\r' {
			p.newLine = true
		} else {
			if p.newLine {
				p.parseCortege()

				p.target = &p.url
				p.url.Truncate(0)
				p.number.Truncate(0)
				p.newLine = false
			}

			p.target.WriteString(string(bytes[i]))
		}
	}
}

func (p *Parser) parseCortege() {
	if p.number.Len() == 0 {
		panic("empty number for url:" + p.url.String())
	}
	value, err := strconv.Atoi(p.number.String())
	if err != nil {
		panic("parse a value: " + err.Error())
	}
	p.collector.process(Cortege{Url: p.url.String(), Rate: value})
}
