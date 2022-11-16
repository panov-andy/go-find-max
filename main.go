package main

import (
	"github.com/panov-andy/go-find-max/clickhouse"
	"log"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("please specify path to file")
	}
	filepath := os.Args[1]

	fileInfo, err := os.Stat(filepath)
	if err != nil {
		panic(err)
	}
	size := fileInfo.Size()
	runtime.NumCPU()
	collector := clickhouse.NewCollector(10)
	parser := clickhouse.NewParser(collector)

	err := clickhouse.ReadFile(filepath, parser)
	if err != nil {
		panic(err)
	}

	parser.Wg.Wait()

	for _, cort := range collector.GetResult() {
		log.Println(cort.Url)
	}

}
