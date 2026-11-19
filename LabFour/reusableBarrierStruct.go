package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type reusableBarrier struct {
	theChan chan bool
	aChan   chan bool
	total   int64
	count   atomic.Int64
}

func createReusableBarrier(N int64) reusableBarrier {
	var c atomic.Int64
	c.Store(0)
	theBarrier := reusableBarrier{
		theChan: make(chan bool),
		aChan:   make(chan bool),
		total:   N,
		count:   c,
	}
	//fmt.Println(theBarrier.total.Load())
	return theBarrier
}

func (b *reusableBarrier) wait() {
	if b.count.Add(1) == b.total {
		//fmt.Println("release1")
		for range b.total - 1 {
			<-b.theChan
		}
	} else {
		b.theChan <- true
	}
	if b.count.Add(-1) == 0 {
		//fmt.Println("release2")
		for range b.total - 1 {
			<-b.aChan
		}
	} else {
		b.aChan <- true
	}
}

func ReusableWorkWithRendezvous(wg *sync.WaitGroup, Num int, theBarrier *reusableBarrier) bool {
	var X time.Duration
	X = time.Duration(rand.IntN(5))
	time.Sleep(X * 10 * time.Millisecond) //wait random time amount
	fmt.Println("Part A", Num)
	//Rendezvous here
	theBarrier.wait()
	fmt.Println("PartB", Num)
	wg.Done()
	return true
}

func LoopWork(wg *sync.WaitGroup, Num int, theBarrier *reusableBarrier, loops int) {
	for range loops {
		ReusableWorkWithRendezvous(wg, Num, theBarrier)
	}
}

func ThreadWork(wg *sync.WaitGroup, Num int, theBarrier *reusableBarrier, threads int) {
	for range threads {
		go ReusableWorkWithRendezvous(wg, Num, theBarrier)
	}
}

func main() {
	var wg sync.WaitGroup
	threadCount := 100
	loopCount := 500
	Rbarrier := createReusableBarrier(int64(threadCount))

	wg.Add(threadCount * loopCount)
	for N := range threadCount {
		go LoopWork(&wg, N, &Rbarrier, loopCount)
	}
	wg.Wait() //wait here until everyone (5 go routines) is done
}
