package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Global variables shared between functions --A BAD IDEA
//var ctxa = context.Background()
//var sema = semaphore.NewWeighted(1)

func WorkWithRendezvous(wg *sync.WaitGroup, Num int, barrier chan bool) bool {
	var X time.Duration
	X = time.Duration(rand.IntN(5))
	time.Sleep(X * time.Second) //wait random time amount
	fmt.Println("Part A", Num)
	//Rendezvous
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
	//sema.Acquire(ctxa, 1)
	var wg sync.WaitGroup
	barrier := make(chan bool)
	threadCount := 2

	wg.Add(threadCount)
	for N := range threadCount {
		go WorkWithRendezvous(&wg, N, barrier)
	}
	wg.Wait() //wait here until everyone (2 go routines) is done

}
