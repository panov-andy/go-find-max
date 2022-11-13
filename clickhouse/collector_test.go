package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCollector_process(t *testing.T) {
	collector := Collector{maxSize: 2, corteges: []Cortege{}}
	collector.process(Cortege{Url: "url1", Rate: 1})
	collector.process(Cortege{Url: "url2", Rate: 2})
	collector.process(Cortege{Url: "url3", Rate: 3})

	assert.Equal(t, collector.corteges[0].Url, "url3")
	assert.Equal(t, collector.corteges[1].Url, "url2")

	collector.process(Cortege{Url: "url4", Rate: 4})

	assert.Equal(t, collector.corteges[0].Url, "url4")
	assert.Equal(t, collector.corteges[1].Url, "url3")
}
