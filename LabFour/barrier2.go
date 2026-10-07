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
	"time"
)

// Place a barrier in this function --use Mutex's and Semaphores
func doStuff(goNum int, max *atomic.Int64, wg *sync.WaitGroup, theChan chan bool, atomvar *atomic.Int64,
	aChan chan bool, atomcount *atomic.Int64) bool {
	time.Sleep(time.Millisecond)
	fmt.Println("Part A", goNum)
	//we wait here until everyone has completed part A
	atomvar.Add(1)
	//fmt.Println(*atomvar, " - ", *max) //troubleshooting
	if *atomvar == *max { //last to arrive -signal others to go
		fmt.Println("release")
		theChan <- true
	} else { //not all here yet we wait until signal
		<-theChan //n-1 wait here
		theChan <- true
	} //end of if-else
	fmt.Println("PartB", goNum)
	atomcount.Add(1)
	if *atomcount == *max {
		fmt.Println("r2")
		atomvar.Store(0)
		aChan <- true //last one through signals continue
		//fmt.Println("release2", *atomvar)
	} else if atomcount.Load() == max.Load()-1 {
		<-theChan //second last through will deal with the extra turnstile signal
		<-aChan   //then wait for continue signal
		aChan <- true
	} else { //n-2 send signal to next routine to continue
		//fmt.Println(goNum, "wait", *atomvar)
		<-aChan //then wait for continue signal
		aChan <- true
	}
	atomcount.Add(-1)
	if atomcount.Load() == 1 {
		<-aChan
	}

	//fmt.Println("out")
	wg.Done()
	return true
} //end-doStuff

func doStuffs(loops int, goNum int, max *atomic.Int64, wg *sync.WaitGroup, theChan chan bool, atomvar *atomic.Int64,
	aChan chan bool, atomcount *atomic.Int64) bool {
	for range loops {
		doStuff(goNum, max, wg, theChan, atomvar, aChan, atomcount)
	}
	return true
}

func main() {
	totalRoutines := 500
	//arrived := 0
	var wg sync.WaitGroup
	loops := 1000
	wg.Add(totalRoutines * loops)
	//we will need some of these
	//var theLock sync.Mutex
	var atomvar atomic.Int64   // atomic variable to use instead of mutex
	var atomcount atomic.Int64 // atomic variable to use instead of mutex
	var atomtot atomic.Int64
	atomtot.Add(int64(totalRoutines)) //need to pass in the totalroutines as atomic for comparison

	theChan := make(chan bool) //use unbuffered channel in place of semaphore
	aChan := make(chan bool)   //would be much easier with a buffer of 1

	/*for i := range totalRoutines { //create the go Routines here
		go doStuff(i, &atomtot, &wg, theChan, &atomvar, aChan, &atomcount)
	}*/

	for i := range totalRoutines { //create multiple go Routines here to test
		go doStuffs(loops, i, &atomtot, &wg, theChan, &atomvar, aChan, &atomcount)
	}

	/* //create 2nd round of go Routines here to test reusability - working for 2 with wait between
	wg.Wait() //wait for everyone to finish before exiting
	wg.Add(totalRoutines)
	for i := range totalRoutines {
		go doStuff(i, &atomtot, &wg, theChan, &atomvar, aChan)
	}*/

	wg.Wait()
} //end-main
