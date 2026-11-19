//Reader/Writer Template Code
//Copyright (C) 2026 Seamus Kennedy

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

// --------------------------------------------
// Author: Seamus Kennedy
// Created on 02/11/2026
// Description:
// A simple Reader-Writer implemented
// Issues:
// None I hope
package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// variable for tracking how many readers currently accessing the data
var readingCount int

// variable to hold the data that will be written to and read from
var data int

func writer(id int, wg *sync.WaitGroup, writeLock chan struct{}, entryLock *sync.Mutex) {
	defer wg.Done()
	var timeBetweenWrites time.Duration
	var writeTime time.Duration
	for {
		timeBetweenWrites = time.Duration(rand.IntN(50))
		writeTime = time.Duration(rand.IntN(50))
		time.Sleep(timeBetweenWrites * 1000 * time.Millisecond) //wait random time amount
		fmt.Println("Writer: ", id, "waiting for", readingCount, "readers to finish to write")
		//writelock prevents writing ensuring that only one writer has access at a time and is also set
		//by the readers to make sure that changes not made after the reader has started working
		//entrylock is used to prevent writer starvation by preventing more readers from starting working once
		//a writer is waiting to write
		entryLock.Lock()
		//writeLock.Lock()
		//replaced writelock mutex with channel, seems multiple writers waiting at entrylock can cause
		//a mutex as the next step to misbehave (see screenshot)
		writeLock <- struct{}{}
		//simulating variable time taken to write
		fmt.Println("Writer: ", id, "writing")
		time.Sleep(writeTime * 10 * time.Millisecond)
		data = id
		fmt.Println("Writer: ", id, "finished writing")
		<-writeLock
		entryLock.Unlock()
	}
}

func reader(id int, wg *sync.WaitGroup, readLock *sync.Mutex, writeLock chan struct{}, entryLock *sync.Mutex) {
	defer wg.Done()
	var timeBetweenReads time.Duration
	var readTime time.Duration
	for {
		timeBetweenReads = time.Duration(rand.IntN(50))
		readTime = time.Duration(rand.IntN(50))
		time.Sleep(timeBetweenReads * 100 * time.Millisecond) //wait random time amount
		//fmt.Println("Reader: ", id, "going to read")
		//entrylock locked then unlocked to check that no writer waiting
		entryLock.Lock()
		entryLock.Unlock()
		//make sure multiple readers don't read readingcount as 0
		readLock.Lock()
		if readingCount == 0 { //actions only done by first reader
			//stopping writers from changing data once readers working
			writeLock <- struct{}{}
			fmt.Println("First reader: ", id)
		}
		readingCount++
		readLock.Unlock()
		time.Sleep(readTime * 10 * time.Millisecond) //wait small random time amount to emulate reading time
		fmt.Println("Reader: ", id, "read data: ", data)
		//decrease readingcount safely and if last out allow writers to continue to writing
		readLock.Lock()
		readingCount--
		if readingCount == 0 {
			fmt.Println("Last reader: ", id)
			<-writeLock
		}
		readLock.Unlock()
	}
}

func main() {
	var wg sync.WaitGroup
	//count of reader threads
	readerCount := 500
	//count of writer threads
	writerCount := 5
	wg.Add(readerCount + writerCount)
	//locks for controlling flow
	var readerLock sync.Mutex
	writerLock := make(chan struct{}, 1)
	var entryLock sync.Mutex
	data = 0 //initialising
	//start writers
	for N := range writerCount {
		go writer(N, &wg, writerLock, &entryLock)
		fmt.Println("start writer:", N)
	}
	//start readers
	for N := range readerCount {
		go reader(N, &wg, &readerLock, writerLock, &entryLock)
		fmt.Println("start reader:", N)
	}
	wg.Wait() //wait here until everyone is done
} //main
