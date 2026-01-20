package main

//
// 启动 coordinator（协调者）进程，具体实现
// 在 ../mr/coordinator.go 中。
//
// 用法：
//     go run mrcoordinator.go pg*.txt
//
// 请不要修改这个文件。
//


import "6.5840/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
