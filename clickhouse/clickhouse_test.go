package clickhouse

import (
	"bytes"
	"fmt"
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
			fmt.Printf("%+v\n", Cortege{Url: url.String(), Value: value})
			url.Truncate(0)
			number.Truncate(0)
		} else {
			target.WriteString(string(char))
		}
	}
}
