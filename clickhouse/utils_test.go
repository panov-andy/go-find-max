package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_readFile(t *testing.T) {
	collector := NewCollector(10)
	parser := NewParser(&collector)
	err := ReadFile("test_data/sample-data.txt", parser)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "http://api.tech.com/item/121345", parser.collector.corteges[0].Url)
	assert.Equal(t, 9, parser.collector.corteges[0].Rate)

	assert.Equal(t, 1, len(parser.collector.corteges))
}

func Test_readFile_empty(t *testing.T) {
	collector := NewCollector(1)
	parser := NewParser(&collector)
	assert.Panics(t, func() {
		ReadFile("test_data/empty-file.txt", parser)
	})
}
func Test_readFile_two_lines(t *testing.T) {
	collector := NewCollector(10)
	parser := NewParser(&collector)
	err := ReadFile("test_data/sample-data-two-lines.txt", parser)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "http://api.tech.com/item/121345", parser.collector.corteges[0].Url)
	assert.Equal(t, 912, parser.collector.corteges[0].Rate)

	assert.Equal(t, 2, len(parser.collector.corteges))
}

//func Test_generate_file(t *testing.T) {
//	file, err := os.Create("../big-file.txt")
//	if err != nil {
//		t.Error(err)
//	}
//	defer file.Close()
//	for i := 0; i <= 10000000; i++ {
//		fmt.Println(i)
//		file.WriteString("url" + fmt.Sprint(i) + " " + fmt.Sprint(i) + "\n")
//	}
//
//}
