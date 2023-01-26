package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hpcloud/tail"
)

func main_single() time.Duration {
	start := time.Now()
	logs := fetchLogs_single()
	logsTransformedSuccess, _ := transformLogs_single(logs)
	logsSended := sendLog_single(logsTransformedSuccess)

	for _, log := range logsSended {
		fmt.Println("logID processed: ", log.id, "- Sended: ", log.sended)
	}
	return time.Since(start)
}

func fetchLogs_single() []string {
	logs := []string{}

	t, err := tail.TailFile("logs.txt", tail.Config{Follow: false})
	if err != nil {
		panic(err)
	}
	for line := range t.Lines {
		logs = append(logs, line.Text)
		// doing some stuffs
		time.Sleep(time.Millisecond * sleepTime)
	}

	return logs
}

func transformLogs_single(logs []string) (successLog []Log, failLog []string) {
	logsTransformedSuccess := []Log{}
	logsTransformedFail := []string{}

	for _, log := range logs {
		logTransformed := Log{}
		if err := json.Unmarshal([]byte(log), &logTransformed); err != nil {
			logsTransformedFail = append(logsTransformedFail, log)
		}
		logsTransformedSuccess = append(logsTransformedSuccess, logTransformed)
		// doing some stuffs
		time.Sleep(time.Millisecond * sleepTime)
	}

	return logsTransformedSuccess, logsTransformedFail
}

func sendLog_single(logs []Log) []LogSended {
	logsSended := []LogSended{}
	for _, log := range logs {

		logSended := LogSended{
			id:     log.Id,
			sended: false,
		}
		if logSended.id%2 == 0 {
			logSended.sended = true
		}

		logsSended = append(logsSended, logSended)
		// doing some stuffs
		time.Sleep(time.Millisecond * sleepTime)
	}

	return logsSended
}
