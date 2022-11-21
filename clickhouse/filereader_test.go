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
	assert.Equal(t, []int64{0, 39, 76, 112, 149, 186}, newLines)
	for index, offset := range newLines {
		_, err := file.Seek(offset, 0)
		if err != nil {
			t.Error(err)
		}
		buff := make([]byte, 8)
		_, err = file.Read(buff)
		if err != nil {
			t.Error(err)
		}
		if index == len(newLines)-2 {
			return
		}
		assert.Equal(t, []byte("https://"), buff)
	}
}

func TestFilePartialRead(t *testing.T) {
	expected := []string{
		"https://api.tech.com/item/121145 95666",
		"https://api.tech.com/item/122245 350",
		"https://api.tech.com/item/123345 25",
		"https://api.tech.com/item/124445 231",
		"https://api.tech.com/item/125545 111"}

	fileToRead := "test_data/sample-data-5lines.txt"
	file, err := os.Open(fileToRead)
	if err != nil {
		t.Error(err)
	}
	endLinesArr, err := FileEndLineSeeker(file, 5)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < len(endLinesArr)-1; i++ {
		err := FilePartialRead(fileToRead, endLinesArr[i], endLinesArr[i+1], func(bytes []byte, endOfFile bool) {
			assert.Equal(t, expected[i], string(bytes))
			assert.Equal(t, true, endOfFile)
		})
		if err != nil {
			t.Error(err)
		}
	}
}
