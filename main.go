package main

import (
	"fmt"
)

type Log struct {
	Id      int
	Message string `json:"message"`
}

type LogSended struct {
	id     int
	sended bool
}

const sleepTime = 50

func main() {
	singleDuration := main_single()
	multiDuration := main_multi()

	fmt.Println("multi duration: ", multiDuration, "- single duration: ", singleDuration)
}
