package main

import (
	"bitcoin"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (net.Conn, error) {
	conn, err := net.Dial("tcp", hostport)
	return conn, err
}

func main() {
	// rand.Seed(time.Now().UnixNano())
	// name := "mlogs_" + strconv.Itoa(rand.Intn(100))
	// flag := os.O_RDWR | os.O_CREATE
	// perm := os.FileMode(0666)
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}
	defer miner.Close()

	// file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		fmt.Println("Couldn't open log file.")
		return
	}
	// LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// _ = LOGF

	// rw := bufio.NewReadWriter(bufio.NewReader(miner), bufio.NewWriter(miner))
	rw := miner
	join, err := json.Marshal(bitcoin.NewJoin())
	rw.Write(join)
	// LOGF.Println("joined")
	// job_count := 0
	for {
		size := reflect.TypeOf(*(bitcoin.NewRequest("", 0, 0))).Size() * 3
		rawResponse := make([]byte, size)
		s, err := rw.Read(rawResponse)
		if err != nil {
			return
		}
		rawResponse = rawResponse[0:s]
		intrfc := bitcoin.NewRequest("", 0, 0)
		_ = json.Unmarshal(rawResponse, intrfc)
		// LOGF.Println("request read", rawResponse)
		// job_count++
		nonce := (*intrfc).Lower
		min := bitcoin.Hash((*intrfc).Data, (*intrfc).Lower)

		for i := intrfc.Lower + 1; i <= intrfc.Upper; i++ {
			res := bitcoin.Hash((*intrfc).Data, i)
			if res < min {
				min = res
				nonce = i
			}
		}

		// LOGF.Println("Computed.")
		response := bitcoin.NewResult(min, nonce)
		mResponse, _ := json.Marshal(response)
		rw.Write(mResponse)
	}
}
