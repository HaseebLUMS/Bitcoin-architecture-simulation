package main

import (
	"bitcoin"
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
)

// const ()

func main() {
	// rand.Seed(time.Now().UnixNano())
	// fs, _ := ioutil.ReadDir("logs/")
	// name := "clogs_" + strconv.Itoa(rand.Intn(100))
	// flag := os.O_RDWR | os.O_CREATE
	// perm := os.FileMode(0666)
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	// file, err := os.OpenFile(name, flag, perm)
	// if err != nil {
	// 	fmt.Println("Couldn't open log file.")
	// 	return
	// }
	// LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// _ = LOGF
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := net.Dial("tcp", hostport)
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()
	_ = message  // Keep compiler happy. Please remove!
	_ = maxNonce // Keep compiler happy. Please remove!
	// LOGF.Println("received (in args): ", message, maxNonce)

	rw := bufio.NewReadWriter(bufio.NewReader(client), bufio.NewWriter(client))
	request, err := json.Marshal(bitcoin.NewRequest(message, 0, maxNonce))
	msg := request
	_, e := rw.Write(msg)
	if e != nil {
		fmt.Println("Error in Write")
		// LOGF.Println("WE\n")
	}
	e = rw.Flush()
	if e != nil {
		fmt.Println("Error in Write")
		// LOGF.Println("WE\n")
	}
	// LOGF.Println("Written: ", msg)
	size := reflect.TypeOf(*(bitcoin.NewResult(0, 0))).Size() * 3
	rawResponse := make([]byte, size)
	s, err := rw.Read(rawResponse)
	if err != nil {
		// LOGF.Println("disconnecting...", err)
		printDisconnected()
		return
	}
	rawResponse = rawResponse[0:s]
	// LOGF.Println("... ", len(rawResponse), s)
	response := bitcoin.NewResult(0, 0)
	err = json.Unmarshal(rawResponse, response)
	// LOGF.Println("=> ", response.Hash, response.Nonce, err)
	printResult(response.Hash, response.Nonce)
	// file.Close()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
