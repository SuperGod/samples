package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
)

func main() {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	// defer f.Close()
	opt := parquet.NewReaderProperties(alloc)
	opt.BufferSize = 1024 * 1024 * 100
	var arrProps pqarrow.ArrowReadProperties
	arrProps.BatchSize = 100000
	arrProps.Parallel = true
	tbl, err := pqarrow.ReadTable(ctx, f, opt, arrProps, memory.DefaultAllocator)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	f.Close()
	fmt.Println(tbl.NumRows(), tbl.NumCols())
	col1 := tbl.Column(0)

	chunks := col1.Data().Chunks()
	data := chunks[0].(*array.String)
	fmt.Println(data.Value(0))
	fmt.Println(data.Value(10))
	time.Sleep(time.Minute)
}
