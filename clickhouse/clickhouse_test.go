package clickhouse

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func Test_f1(t *testing.T) {
	var src = `http://api.tech.com/item/121345 9
http://api.tech.com/item/122345 350
http://api.tech.com/item/123345 25
http://api.tech.com/item/124345 231
http://api.tech.com/item/125345 111
`

	url := &bytes.Buffer{}
	number := &bytes.Buffer{}
	target := url

	for _, char := range src {
		if char == ' ' {
			target = number
		} else if char == '\n' {
			target = url
			value, err := strconv.Atoi(number.String())
			if err != nil {
				t.Errorf("parse a value: %e", err)
			}
			fmt.Printf("%+v\n", Cortege{Url: url.String(), Rate: value})
			url.Truncate(0)
			number.Truncate(0)
		} else {
			target.WriteString(string(char))
		}
	}
}

func Test_sortByRate(t *testing.T) {
	var c = []Cortege{
		{Url: "http://api.tech.com/item/121345", Rate: 9},
		{Url: "http://api.tech.com/item/122345", Rate: 350},
		{Url: "http://api.tech.com/item/123345", Rate: 25},
		{Url: "http://api.tech.com/item/124345", Rate: 231},
		{Url: "http://api.tech.com/item/125345", Rate: 111},
	}

	sortByRate(c)

	assert.Equal(t, "http://api.tech.com/item/122345", c[0].Url)
	assert.Equal(t, "http://api.tech.com/item/124345", c[1].Url)
}

func Test_readFile(t *testing.T) {
	parser := Parser{}
	err := ReadFile("test_data/sample-data.txt", &parser)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "http://api.tech.com/item/121345 9", parser.content())
}

func Test_readFile_empty(t *testing.T) {
	parser := Parser{}
	err := ReadFile("test_data/empty-file.txt", &parser)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "", parser.content())
}
func Test_readFile_two_lines(t *testing.T) {
	parser := Parser{}
	err := ReadFile("test_data/sample-data-two-lines.txt", &parser)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "http://api.tech.com/item/121345 9\r\nhttp://api.tech.com/item/121345 912", parser.content())
}
