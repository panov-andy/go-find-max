package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
