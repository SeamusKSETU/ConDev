// author: Seamus Kennedy C00209242
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Global variables shared between functions --A BAD IDEA
var wga sync.WaitGroup

//wg updated to wga to avoid same name usage from mutex.go

func addsAtomic(n int, total *atomic.Int64) bool {
	for i := 0; i < n; i++ {
		total.Add(1)
	}
	wga.Done() //let waitgroup know we have finished
	return true
}

func main() {

	var total atomic.Int64

	//for loop using range option
	for i := range 10 {
		//the waitgroup is used as a barrier
		// init to number of go routines
		wga.Add(1)
		fmt.Println("go Routine ", i)
		go addsAtomic(1000, &total)
	}
	wga.Wait() //wait here until everyone (10 go routines) is done
	fmt.Println(total.Load())

}
