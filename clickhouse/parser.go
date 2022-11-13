package clickhouse

import (
	"bytes"
	"strconv"
)

func NewParser(collector *Collector) Parser {
	url := bytes.Buffer{}
	number := bytes.Buffer{}

	return Parser{
		collector: collector,
		buffer:    bytes.Buffer{},
		url:       url,
		number:    number,
		target:    &url,
		newLine:   false,
	}
}

type Parser struct {
	collector *Collector
	buffer    bytes.Buffer

	url     bytes.Buffer
	number  bytes.Buffer
	target  *bytes.Buffer
	newLine bool
}

func (p *Parser) submitChunk(bytes []byte, readBytes int) {
	p.buffer.WriteString(string(bytes[:readBytes]))

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

func (p *Parser) content() string {
	return p.buffer.String()
}

func (p *Parser) parseCortege() {
	value, err := strconv.Atoi(p.number.String())
	if err != nil {
		panic("parse a value: " + err.Error())
	}
	p.collector.process(Cortege{Url: p.url.String(), Rate: value})
}
