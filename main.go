package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hpcloud/tail"
	"golang.org/x/sync/semaphore"
)

type Event struct {
	Id   int
	Name string `json:"name"`
}

type EventSended struct {
	id     int
	sended bool
}

func main() {
	ctx := context.Background()

	events := fetchEvents(ctx, 1)
	eventsTransformed := transformEvents(ctx, events, 1)
	eventsSended := sendEvents(ctx, eventsTransformed, 1)

	for event := range eventsSended {
		if !event.sended {
			fmt.Println("fail to send event: ", event.id)
		} else {
			fmt.Println("success to send event: ", event.id)

		}
	}
}

func fetchEvents(ctx context.Context, threads int64) <-chan string {
	events := make(chan string)
	sem := semaphore.NewWeighted(threads)

	go func() {
		for {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}
			go func() {
				defer sem.Release(1)

				t, err := tail.TailFile("event.txt", tail.Config{Follow: true})
				if err != nil {
					panic(err)
				}

				for line := range t.Lines {
					events <- line.Text
				}

				time.Sleep(time.Second * 1)
			}()
		}
	}()

	return events
}

func transformEvents(ctx context.Context, events <-chan string, threads int64) <-chan Event {
	eventsTransformed := make(chan Event)
	sem := semaphore.NewWeighted(threads)

	go func() {
		for event := range events {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}
			go func() {
				defer sem.Release(1)

				eventTransformed := Event{}
				if err := json.Unmarshal([]byte(event), &eventTransformed); err != nil {
					fmt.Println("Failed to unmarshal event", err)
				}
				eventsTransformed <- eventTransformed
				time.Sleep(time.Second * 1)
			}()
		}
	}()

	return eventsTransformed
}

func sendEvents(ctx context.Context, events <-chan Event, threads int64) <-chan EventSended {
	eventsSended := make(chan EventSended)
	sem := semaphore.NewWeighted(threads)

	go func() {
		for event := range events {
			if err := sem.Acquire(ctx, 1); err != nil {
				fmt.Println("Failed to acquire semaphore")
				break
			}
			go func() {
				defer sem.Release(1)

				eventSended := EventSended{
					id:     event.Id,
					sended: false,
				}
				if eventSended.id%2 == 0 {
					eventSended.sended = true
				}

				eventsSended <- eventSended
				time.Sleep(time.Second * 1)
			}()
		}
	}()

	return eventsSended
}
