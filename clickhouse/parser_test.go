package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parser1(t *testing.T) {
	parser := NewParser(nil)
	str := "h"
	parser.submitChunk([]byte(str), 1)

	assert.Equal(t, "h", parser.url.String())
	assert.Equal(t, "h", parser.target.String())
}

func Test_parser2(t *testing.T) {
	parser := NewParser(nil)
	parser.submitChunk([]byte("h"), 1)
	assert.Equal(t, "h", parser.url.String())
	assert.Equal(t, "h", parser.target.String())

	parser.submitChunk([]byte(" "), 1)
	parser.submitChunk([]byte("4"), 1)

	assert.Equal(t, "4", parser.number.String())
	assert.Equal(t, "4", parser.target.String())
}

func Test_parser3(t *testing.T) {
	collector := NewCollector(1)

	parser := NewParser(collector)
	parser.submitChunk([]byte("h"), 1)
	parser.submitChunk([]byte(" "), 1)
	parser.submitChunk([]byte("4"), 1)
	parser.submitChunk([]byte("\n"), 1)

	parser.parseCortege()
	assert.Equal(t, "h", collector.GetResult()[0].Url)
	assert.Equal(t, 4, collector.GetResult()[0].Rate)
}

func Test_parser4(t *testing.T) {
	collector := NewCollector(1)
	parser := NewParser(collector)
	parser.submitChunk([]byte{}, 0)
	assert.Panics(t, parser.parseCortege)
}
