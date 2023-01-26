package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hpcloud/tail"
	"golang.org/x/sync/semaphore"
)

func main_multi() time.Duration {
	ctx := context.Background()
	start := time.Now()
	logs := fetchLogs(ctx, 10)
	logsTransformedSuccess, _ := transformLogs(ctx, logs, 10)
	logsSended := sendLog(ctx, logsTransformedSuccess, 10)

	for log := range logsSended {
		fmt.Println("logID processed: ", log.id, "- Sended: ", log.sended)
	}
	return time.Since(start)
}

func fetchLogs(ctx context.Context, threads int64) <-chan string {
	logs := make(chan string)
	sem := semaphore.NewWeighted(threads)

	go func() {
		t, err := tail.TailFile("logs.txt", tail.Config{Follow: false})
		if err != nil {
			panic(err)
		}

		for line := range t.Lines {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}

			go func(log string) {
				defer sem.Release(1)
				logs <- log
				// doing some stuffs
				time.Sleep(time.Millisecond * sleepTime)
			}(line.Text)
		}
		if err := sem.Acquire(ctx, threads); err != nil {
			fmt.Println("Failed to reset semaphore")
		}

		fmt.Println("close fetch channels")
		close(logs)
	}()

	return logs
}

func transformLogs(ctx context.Context, logs <-chan string, threads int64) (successLog <-chan Log, failLog <-chan string) {
	logsTransformedSuccess := make(chan Log)
	logsTransformedFail := make(chan string)
	sem := semaphore.NewWeighted(threads)

	go func() {
		for log := range logs {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}
			go func(log string) {
				defer sem.Release(1)

				logTransformed := Log{}
				if err := json.Unmarshal([]byte(log), &logTransformed); err != nil {
					logsTransformedFail <- log
				} else {
					logsTransformedSuccess <- logTransformed
				}
				// doing some stuffs
				time.Sleep(time.Millisecond * sleepTime)
			}(log)
		}
		if err := sem.Acquire(ctx, threads); err != nil {
			fmt.Println("Failed to reset semaphore")
		}

		fmt.Println("close transformed channels")
		close(logsTransformedSuccess)
		close(logsTransformedFail)
	}()

	return logsTransformedSuccess, logsTransformedFail
}

func sendLog(ctx context.Context, logs <-chan Log, threads int64) <-chan LogSended {
	logsSended := make(chan LogSended)
	sem := semaphore.NewWeighted(threads)

	go func() {
		for log := range logs {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}
			go func(log Log) {
				defer sem.Release(1)

				logSended := LogSended{
					id:     log.Id,
					sended: false,
				}
				if logSended.id%2 == 0 {
					logSended.sended = true
				}

				logsSended <- logSended
				// doing some stuffs
				time.Sleep(time.Millisecond * sleepTime)
			}(log)
		}
		if err := sem.Acquire(ctx, threads); err != nil {
			fmt.Println("Failed to reset semaphore")
		}

		fmt.Println("close sended channel")
		close(logsSended)
	}()

	return logsSended
}
