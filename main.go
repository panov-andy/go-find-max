package main

import (
	"fmt"
	"github.com/panov-andy/go-find-max/clickhouse"
	"log"
	"os"
	"runtime"
	"sync"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("please specify path to file")
	}
	filepath := os.Args[1]

	fmt.Printf("AMOUNT OF CPU: %d\n", runtime.NumCPU())

	endLines, err := clickhouse.FileEndLineSeekerByPath(filepath)
	if err != nil {
		panic(err)
	}

	waitGroup := sync.WaitGroup{}
	collectors := make([]*clickhouse.Collector, 0)

	for i := 0; i < len(endLines)-2; i++ {
		collector := clickhouse.NewCollector(1)
		collectors = append(collectors, collector)

		parser := clickhouse.NewParser(collector)

		waitGroup.Add(1)
		index := i
		go func() {
			err := clickhouse.FilePartialRead(filepath, endLines[index], endLines[index+1], func(bytes []byte, endOfFile bool) {
				//fmt.Printf("endFile %s, SUB: %s\n", endOfFile, string(bytes))
				parser.SubmitChunk(bytes, len(bytes))
				if endOfFile {
					parser.ParseCortege()
					waitGroup.Done()
				}
			})
			if err != nil {
				panic(err)
			}
		}()
	}
	waitGroup.Wait()

	corteges := make([]clickhouse.Cortege, 0)
	for _, collector := range collectors {
		for _, cortege := range collector.GetResult() {
			corteges = append(corteges, cortege)
		}
	}

	clickhouse.SortByRate(corteges)
	corteges = corteges[:10]
	for _, cortege := range corteges {
		fmt.Printf("%v\n", cortege)
	}
}
