package main

import (
	"fmt"
	"time"
)

func main() {
	ticker := time.NewTicker(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-ticker.C:
					println(fmt.Sprintf("goroutine %d\n", i))
				}
			}
		}()
	}

	ch := make(chan struct{})
	<-ch
}
