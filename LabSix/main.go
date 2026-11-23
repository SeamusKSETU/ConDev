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
// A simple Producer-Consumer implemented
// Issues:
// None I hope
package main

import (
	"fmt"
	"math/rand"
	"sync"
)

// A simple Reader-Writer implemented, thought this was what the lab was on, whoops, commented out
/*
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
*/

type Buffer struct {
	data                []int
	producercount, size int
	consumerexit        bool
	accesslock          chan struct{}
	readlock            chan struct{}
	writelock           chan struct{}
	writecount          chan struct{}
	readcount           chan struct{}
}

func createBuffer(size int) Buffer {
	//empty readlock semaphore on creation
	theBuffer := Buffer{
		writecount:    make(chan struct{}, size),
		readcount:     make(chan struct{}, size),
		data:          make([]int, 0, size),
		accesslock:    make(chan struct{}, 1),
		readlock:      make(chan struct{}, 1),
		writelock:     make(chan struct{}, 1),
		producercount: 0,
		consumerexit:  false,
		size:          size,
	}
	return theBuffer
}

func (b *Buffer) Add(add []int, id int) {
	fmt.Println("Producer", id, "going to add", add)
	b.writelock <- struct{}{}
	for range add {
		b.writecount <- struct{}{}
	}
	b.accesslock <- struct{}{}
	fmt.Println("Producer", id, "adding", add)
	b.data = append(b.data, add...)
	fmt.Println("Producer", id, "done adding, current data:", b.data)
	<-b.accesslock
	for range add {
		b.readcount <- struct{}{}
	}
	<-b.writelock
}

func (b *Buffer) registerProducer() {
	b.accesslock <- struct{}{}
	b.producercount++
	<-b.accesslock
}

func (b *Buffer) finishProducer() {
	b.accesslock <- struct{}{}
	b.producercount--
	if b.producercount == 0 {
		fmt.Println("Last Producer done. Current data:", b.data)
		<-b.accesslock
		//filling readcount to make sure consumers don't get stuck in the check below
		for range b.size {
			b.readcount <- struct{}{}
		}
	} else {
		<-b.accesslock
	}
}

func (b *Buffer) Get(size, id int) []int {
	fmt.Println("Consumer", id, "getting", size, "data")
	out := make([]int, 0)
	b.readlock <- struct{}{}
	fmt.Println("Consumer", id, "checking enough data available")
	if b.consumerexit {
		fmt.Println("Consumer", id, "exiting")
		<-b.readlock
		//returning a slice of -1 indicates consuming finished
		return []int{-1}
	}
	//checking enough data produced for consume request before continuing
	for range size {
		<-b.readcount
	}
	b.accesslock <- struct{}{}
	fmt.Println("Consumer", id, "accessing data")
	//case for last consumer through
	if b.producercount == 0 && size >= len(b.data) {
		fmt.Println("Final data retrieved:", b.data)
		out = b.data
		b.consumerexit = true
		<-b.accesslock
		<-b.readlock
	} else {
		//normal consumer functioning
		out = b.data[0:size]
		b.data = b.data[size:]

		fmt.Println("Consumer", id, "done getting", size, "data:", out)
		<-b.accesslock
		//letting writers know more space available
		for range size {
			<-b.writecount
		}
		<-b.readlock
	}
	return out
}

func Producer(buffer *Buffer, wg *sync.WaitGroup, loops, max, id int) {
	defer wg.Done()
	buffer.registerProducer()
	defer buffer.finishProducer()
	data := make([]int, 0)
	var random int
	for range loops {
		random = 1 + rand.Intn(max-1)
		for range random {
			data = append(data, rand.Intn(100))
		}
		buffer.Add(data, id)
		data = data[:0]
	}
	fmt.Println("Producer", id, "finished")
}

func Consumer(buffer *Buffer, wg *sync.WaitGroup, id, max int) {
	defer wg.Done()
	data := make([]int, 0)
	size := 0
	for {
		size = 1 + rand.Intn(max-1)
		data = buffer.Get(size, id)
		if len(data) == 1 && data[0] == -1 {
			break
		}
		if len(data) < size {
			break
		}
		fmt.Println("Consumer", id, "consumed data:", data)
	}
	fmt.Println("Consumer", id, "finished")
}

func mainTakingArgumentsToTest(runNumber, maxsizeproduced, maxsizeconsumed, producerCount, consumerCount, producerloops int) {
	//maxsizeproduced := 10
	//maxsizeconsumed := 5
	buffersize := maxsizeproduced + maxsizeconsumed
	b := createBuffer(buffersize)
	var wg sync.WaitGroup

	//count of producer threads
	//producerCount := 5
	//producerloops := 100

	//count of consumer threads
	//consumerCount := 5
	wg.Add(producerCount + consumerCount)
	//locks for controlling flow
	//start producers
	for N := range producerCount {
		go Producer(&b, &wg, producerloops, maxsizeproduced, runNumber*1000+N)
		fmt.Println("start producer:", runNumber*1000+N)
	}
	//start consumers
	for N := range consumerCount {
		go Consumer(&b, &wg, runNumber*1000+N, maxsizeconsumed)
		fmt.Println("start consumer:", runNumber*1000+N)
	}
	wg.Wait()
}

func main() {
	runs := 1000
	randomvalues := make([]int, 5)
	runinfo := make([][]int, runs)
	for i := range runs {
		runinfo[i] = make([]int, 0)
	}
	for i := range runs {
		//maxsizes can't be allowed to be 1 since they get passed into randIntn minus 1 above, 0 argument is invalid
		randomvalues[0] = 2 + rand.Intn(98)
		randomvalues[1] = 2 + rand.Intn(98)
		//all other values have a lower limit of 1
		randomvalues[2] = 1 + rand.Intn(99)
		randomvalues[3] = 1 + rand.Intn(99)
		randomvalues[4] = 1 + rand.Intn(999)
		fmt.Println("run number:", i, "random values:", randomvalues)
		mainTakingArgumentsToTest(i, randomvalues[0], randomvalues[1], randomvalues[2], randomvalues[3], randomvalues[4])
		runinfo[i] = append(runinfo[i], randomvalues...)
	}
	for i := range runs {
		fmt.Println("Run", i, "runinfo:", runinfo[i])
	}
}
