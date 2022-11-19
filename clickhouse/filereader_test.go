package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFindOffsetAfterNewLine(t *testing.T) {
	str := `http://api.tech.com/item/121345 9
http://api.tech.com/item/125345 111`

	newLineIndex := FindOffsetAfterNewLine([]byte(str), len(str))
	assert.Equal(t, int64(34), newLineIndex)

	newLineIndex = FindOffsetAfterNewLine([]byte(str), 20)
	assert.Equal(t, int64(-1), newLineIndex)
}

func TestFileEndLineSeeker(t *testing.T) {
	file, err := os.Open("test_data/sample-data-5lines.txt")
	if err != nil {
		t.Error(err)
	}
	newLines, err := FileEndLineSeeker(file, int64(5))
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []int64{0, 39, 76, 112, 149}, newLines)
	for _, offset := range newLines {
		_, err := file.Seek(offset, 0)
		if err != nil {
			t.Error(err)
		}
		buff := make([]byte, 8)
		_, err = file.Read(buff)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []byte("https://"), buff)
	}
}
