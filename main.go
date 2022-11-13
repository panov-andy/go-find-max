package main

import (
	"github.com/panov-andy/go-find-max/clickhouse"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("please specify path to file")
	}
	filepath := os.Args[1]

	collector := clickhouse.NewCollector(2)
	parser := clickhouse.NewParser(&collector)

	err := clickhouse.ReadFile(filepath, &parser)
	if err != nil {
		panic(err)
	}

	for _, cort := range collector.GetResult() {
		log.Println(cort.Url)
	}

}
