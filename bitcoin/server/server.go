/*
1) Whenever a job comes:
	a) it is broken to chunks of size maxChunk (last chunk might be lesser in size),
	b) It is checked whether miners are present or free
	c) if yes, chunks are assigned to miners
2) Whenever a miner comes:
	a) It checks for jobs
	b) If present, it starts doing them
	c) When a job is done, it probs for next job
	d) if connection lost, return status oj job as minerError
3) When a miner fails: controller returns the job to the job queue
4) Fairness:
	Job A = 1, 2, 3, 4 (chunks)
	Job B = 5, 6, 7, 8 (chunks)
	Order of processing => 1, 5, 2, 6, 3, 7, 4, 8
*/
package main

import (
	"bitcoin"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
)

// const maxChunk = 85000
const maxChunk = 50000

type server struct {
	listener net.Listener
}

type client struct {
	rw     net.Conn
	intrfc *bitcoin.Message
	chunks int //remaining pieces
	nonce  uint64
	hash   uint64
	pieces int //total pieces
}
type miner struct {
	rw   net.Conn
	free bool
}
type task struct { //miner solving a piece of job is referred as task
	minerID int
	j       job
	nonce   uint64
	hash    uint64
	status  statusType
}
type job struct {
	clientID    int
	chunkID     int
	chunkNumber int
	data        string
	lower       uint64
	upper       uint64
	assigned    bool
	nonce       uint64
	hash        uint64
}

func startServer(port int) (*server, error) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	return &server{ln}, err
}

//status of task being solved by a miner
type statusType int

const (
	safe statusType = iota
	minerError
	apiError //json marshalling ...
)

// //LOGF is used for logging
// var LOGF *log.Logger

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}
	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)
	defer srv.listener.Close()

	submitMiner := make(chan net.Conn)
	submitClientWithJob := make(chan client)
	getJob := make(chan job)
	jobCompleted := make(chan job)
	triggerMiners := make(chan int)
	returnJob := make(chan job)

	go handleMinersQueue(submitMiner, getJob, jobCompleted, triggerMiners, returnJob)
	go handleClientsJobsQueue(submitClientWithJob, getJob, jobCompleted, triggerMiners, returnJob)

	for {
		conn, e := srv.listener.Accept()
		intrfc := &(bitcoin.Message{0, "", 0, 0, 0, 0})
		size := int(reflect.TypeOf(*intrfc).Size()) * 3
		msg := make([]byte, size)
		s, e := conn.Read(msg)
		if e != nil {
			fmt.Println("Error", e)
		}
		if s > len(msg) {
		} else {
			msg = msg[0:s]
		}
		err = json.Unmarshal(msg, intrfc)
		if err != nil {
		}
		if intrfc.Type == bitcoin.Join {
			submitMiner <- conn
		}
		if intrfc.Type == bitcoin.Request {
			submitClientWithJob <- client{conn, intrfc, -1, 0, 18446744073709551615, 0}
		}
	}
}

func dispatchJob(minerID int, conn net.Conn, taskCompleted chan task, j job) {
	r := bitcoin.NewRequest(j.data, j.lower, j.upper)
	req, e := json.Marshal(r)
	if e != nil {
		taskCompleted <- task{minerID, j, r.Nonce, r.Hash, apiError}
		return
	}
	_, e = conn.Write(req)
	if e != nil {
		taskCompleted <- task{minerID, j, r.Nonce, r.Hash, minerError}
		return
	}
	r = bitcoin.NewResult(0, 0)
	size := reflect.TypeOf(*r).Size() * 3
	rr := make([]byte, size)
	s, e := conn.Read(rr)
	if e != nil {
		taskCompleted <- task{minerID, j, r.Nonce, r.Hash, minerError}
		return
	}
	if s > len(rr) {
		taskCompleted <- task{minerID, j, r.Nonce, r.Hash, apiError}
		return
	}
	rr = rr[0:s]

	e = json.Unmarshal(rr, r)
	if e != nil {
		taskCompleted <- task{minerID, j, r.Nonce, r.Hash, apiError}
		return
	}
	taskCompleted <- task{minerID, j, r.Nonce, r.Hash, safe}
}

func handleMinersQueue(submitMiner chan net.Conn, getJob chan job, jobCompleted chan job, triggerMiners chan int, returnJob chan job) {

	minersMap := make(map[int]miner)
	taskCompleted := make(chan task)
	for {
		MID := -1
		select {
		case rw := <-submitMiner:
			minerID := len(minersMap)
			minersMap[minerID] = miner{rw, false}
			MID = minerID
			getJob <- job{}
			j := <-getJob
			if j.clientID == -1 {
				tmp := minersMap[MID]
				minersMap[MID] = miner{tmp.rw, true}
			} else {
				go dispatchJob(MID, minersMap[MID].rw, taskCompleted, j)
			}
		case t := <-taskCompleted:
			if t.status == safe {
				jobCompleted <- job{t.j.clientID, t.j.chunkID, t.j.chunkNumber, t.j.data, t.j.lower, t.j.upper, t.j.assigned, t.nonce, t.hash}
				tmp := minersMap[t.minerID]
				minersMap[t.minerID] = miner{tmp.rw, false}
				MID = t.minerID
				getJob <- job{}
				j := <-getJob
				if j.clientID == -1 {
					tmp := minersMap[MID]
					minersMap[MID] = miner{tmp.rw, true}
				} else {
					go dispatchJob(MID, minersMap[MID].rw, taskCompleted, j)
				}
			} else if t.status == minerError {
				returnJob <- t.j
			}
		case <-triggerMiners:
			for i, m := range minersMap {
				if m.free {
					MID = i
					minersMap[i] = miner{m.rw, false}
					getJob <- job{}
					j := <-getJob
					if j.clientID == -1 {
						tmp := minersMap[MID]
						minersMap[MID] = miner{tmp.rw, true}
					} else {
						go dispatchJob(MID, minersMap[MID].rw, taskCompleted, j)
					}
				}
			}
		}
	}
}

func handleClientsJobsQueue(submitClientWithJob chan client, getJob chan job, jobCompleted chan job, triggerMiners chan int, returnJob chan job) {
	clientJobMap := make(map[int]client)
	chunkedJobMap := make(map[int]job)

	nextJobToServe := 0
	for {
		select {
		case cl := <-submitClientWithJob:
			clID := len(clientJobMap)
			chunkNumber := 1
			i := cl.intrfc.Lower
			for i = cl.intrfc.Lower; i <= cl.intrfc.Upper; i += maxChunk {
				chunkID := len(chunkedJobMap)
				upperLim := i + maxChunk - 1
				if upperLim > cl.intrfc.Upper {
					upperLim = cl.intrfc.Upper
				}
				chunkedJobMap[chunkID] = job{clID, chunkID, chunkNumber, cl.intrfc.Data, i, upperLim, false, 0, 18446744073709551615}
				chunkNumber++
			}
			clientJobMap[clID] = client{cl.rw, cl.intrfc, chunkNumber - 1, cl.nonce, cl.hash, chunkNumber - 1}
			go func() { triggerMiners <- 1 }() //later in time , trigger a check
		case <-getJob:
			fairIndex := 0
			for fi := 0; fi < nextJobToServe; fi++ {
				fairIndex += clientJobMap[fi].pieces
			}
			jobServed := false
			var keys []int
			for k := range chunkedJobMap {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, i := range keys {
				j := chunkedJobMap[i]
				if i >= fairIndex && j.assigned == false {
					chunkedJobMap[i] = job{j.clientID, j.chunkID, j.chunkNumber, j.data, j.lower, j.upper, true, 0, 18446744073709551615}
					getJob <- chunkedJobMap[i]
					jobServed = true
					nextJobToServe++
					if nextJobToServe >= len(clientJobMap) {
						nextJobToServe = 0
					}
					break
				} else {
				}
			}
			if !jobServed {
				for _, i := range keys {
					j := chunkedJobMap[i]
					if j.assigned == false {
						chunkedJobMap[i] = job{j.clientID, j.chunkID, j.chunkNumber, j.data, j.lower, j.upper, true, 0, 18446744073709551615}
						getJob <- chunkedJobMap[i]
						jobServed = true
						break
					}
				}
				if !jobServed {
					getJob <- job{-1, -1, -1, "", 0, 0, false, 0, 0}
				}
			}
		case j := <-returnJob:
			chunkedJobMap[j.chunkID] = job{j.clientID, j.chunkID, j.chunkNumber, j.data, j.lower, j.upper, false, 0, 18446744073709551615}
			go func() { triggerMiners <- 1 }()
		case j := <-jobCompleted:
			tmp := clientJobMap[j.clientID]
			var h uint64 = 0
			var n uint64 = 0
			if j.hash < tmp.hash {
				h = j.hash
				n = j.nonce
			} else {
				h = tmp.hash
				n = tmp.nonce
			}
			clientJobMap[j.clientID] = client{tmp.rw, tmp.intrfc, tmp.chunks - 1, n, h, tmp.pieces}
			if clientJobMap[j.clientID].chunks == 0 {
				res, _ := json.Marshal(bitcoin.NewResult(h, n))
				clientJobMap[j.clientID].rw.Write(res)
			}
		}
	}
}
