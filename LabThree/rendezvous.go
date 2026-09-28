package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Global variables shared between functions --A BAD IDEA
//var ctxa = context.Background() //in here for testing
//var sema = semaphore.NewWeighted(0)

func WorkWithRendezvous(wg *sync.WaitGroup, Num int, barrier chan bool) bool {
	var X time.Duration
	X = time.Duration(rand.IntN(5))
	time.Sleep(X * time.Second) //wait random time amount
	fmt.Println("Part A", Num)
	//Rendezvous here
	if Num == 0 {
		barrier <- true
		//time.Sleep(10 * time.Second)
		//sema.Release(1)
	} else {
		<-barrier
		//sema.Acquire(ctxa, 1)
	}

	fmt.Println("PartB", Num)
	wg.Done()
	return true
}

func main() {
	var wg sync.WaitGroup
	barrier := make(chan bool)
	threadCount := 2

	wg.Add(threadCount)
	for N := range threadCount {
		go WorkWithRendezvous(&wg, N, barrier)
	}
	wg.Wait() //wait here until everyone (10 go routines) is done

}
