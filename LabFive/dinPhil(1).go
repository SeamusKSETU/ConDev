// Dining Philosophers Template Code
// Author: Joseph Kehoe
// Created: 21/10/24
// Modified by: Seamus Kennedy
//GPL Licence
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
// Description:
// A simple implementation of the dining philosophers problem

package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func think(index int) {
	var X time.Duration
	X = time.Duration(rand.IntN(5))
	time.Sleep(X * 10 * time.Millisecond) //wait random time amount
	fmt.Println("Phil: ", index, "was thinking")
}

func eat(index int) {
	var X time.Duration
	X = time.Duration(rand.IntN(5))
	time.Sleep(X * 1 * time.Millisecond) //wait random time amount
	fmt.Println("Phil: ", index, "was eating")
}

// for sending waits to the channels
func getForks(index int, forks map[int]chan bool) {
	forks[index] <- true
	//don't need to modulo by the thread count when 1 added since the one that would be out of
	//bounds is being handled by getForksLeft and has 0 hardcoded
	forks[index+1] <- true
	//print for troubleshooting purposes
	//fmt.Println(index, index+1, "got got")
}

// for sending wait to channels on final thread
func getForksLeft(index int, forks map[int]chan bool) {
	forks[0] <- true
	forks[index] <- true
	//fmt.Println(index, 0, "got got")
}

// for signalling the channels on threads
func putForks(index int, forks map[int]chan bool) {
	<-forks[index]
	<-forks[index+1]
	//fmt.Println(index, index+1, "got put")
}

// for signalling the channels on the final thread
func putForksLeft(index int, forks map[int]chan bool) {
	<-forks[index]
	<-forks[0]
	//fmt.Println(index, 0, "got put")
}

func doPhilStuff(index int, wg *sync.WaitGroup, forks map[int]chan bool) {
	for {
		think(index)
		getForks(index, forks)
		eat(index)
		putForks(index, forks)
	}
	wg.Done()
}

func doPhilLeftStuff(index int, wg *sync.WaitGroup, forks map[int]chan bool) {
	for {
		think(index)
		getForksLeft(index, forks)
		eat(index)
		putForksLeft(index-1, forks)
	}
	wg.Done()
}

func main() {
	var wg sync.WaitGroup
	//testing with higher thread counts to make sure all working
	philCount := 200
	wg.Add(philCount)

	forks := make(map[int]chan bool)
	//set up forks channels with 1 space max
	for k := range philCount {
		forks[k] = make(chan bool, 1)
	}
	//start philosophers except the last
	for N := range philCount - 1 {
		go doPhilStuff(N, &wg, forks)
		fmt.Println("start:", N)
	}
	//starts the left handed philosopher, having a separate function is more efficient
	//than having an if statement for index==0 being checked by all threads
	go doPhilLeftStuff(philCount-1, &wg, forks)
	fmt.Println("start:", philCount-1)
	wg.Wait() //wait here until everyone (10 go routines) is done

} //main
