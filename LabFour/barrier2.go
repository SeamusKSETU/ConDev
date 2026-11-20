//Barrier.go Template Code
//Copyright (C) 2024 Dr. Joseph Kehoe

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

//--------------------------------------------
// Author: Joseph Kehoe (Joseph.Kehoe@setu.ie)
// Created on 30/9/2024
// Modified by: Seamus Kennedy
// Description:
// A simple barrier implemented using mutex and unbuffered channel
// Issues:
// None I hope
//1. Change mutex to atomic variable
//2. Make it a reusable barrier
//--------------------------------------------

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Place a barrier in this function --use Mutex's and Semaphores
func doStuff(goNum int, max int64, wg *sync.WaitGroup, theChan chan bool, atomvar *atomic.Int64,
	aChan chan bool) bool {
	//time.Sleep(time.Millisecond)
	fmt.Println("Part A", goNum)
	//we wait here until everyone has completed part A
	//fmt.Println(*atomvar, " - ", *max) //troubleshooting
	if atomvar.Add(1) == max { //last to arrive -signal others to go
		fmt.Println("release 1")
		for range max - 1 {
			theChan <- true
		}
	} else { //not all here yet we wait until signal
		<-theChan //n-1 wait here
	}
	fmt.Println("PartB", goNum)

	if atomvar.Add(-1) == 0 {
		fmt.Println("release 2")
		for range max - 1 {
			aChan <- true //last one through signals continue
		}
	} else { //n-1 wait to continue
		//fmt.Println(goNum, "wait", *atomvar)
		<-aChan //wait for continue signal
	}
	wg.Done()
	return true
} //end-doStuff

func doStuffs(loops int, goNum int, max int64, wg *sync.WaitGroup, theChan chan bool, atomvar *atomic.Int64,
	aChan chan bool) bool {
	//testing barrier reusability by calling the functions multiple times
	for range loops {
		doStuff(goNum, max, wg, theChan, atomvar, aChan)
	}
	return true
}

func main() {
	totalRoutines := 5
	//arrived := 0
	var wg sync.WaitGroup
	loops := 200
	wg.Add(totalRoutines * loops)
	//we will need some of these
	//var theLock sync.Mutex
	var atomvar atomic.Int64 // atomic variable to use instead of mutex

	theChan := make(chan bool) //use unbuffered channel in place of semaphore
	aChan := make(chan bool)   //would be much easier with a buffer of 1

	for i := range totalRoutines { //create multiple go Routines here to test reusability
		go doStuffs(loops, i, int64(totalRoutines), &wg, theChan, &atomvar, aChan)
	}

	wg.Wait()
} //end-main
