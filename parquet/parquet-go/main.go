package main

import (
	"log"
	"os"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	cacheFile := os.Args[1]
	fr, err := local.NewLocalFileReader(cacheFile)
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}
	pr.ReadStop()
	fr.Close()
	log.Println("len:", len(res))
	log.Println("data:", res[0])
	time.Sleep(time.Minute)
}
