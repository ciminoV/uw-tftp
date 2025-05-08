package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"pack.ag/tftp"
)

func main() {
	ip := os.Args[1]
	port := os.Args[2] // Server port
	tcpPort := os.Args[3]
	opts := []tftp.ServerOpt{
		tftp.ServerRetransmit(5),                                 // default 5
		tftp.ServerSinglePort(true),                              // default false
		tftp.ServerTcpForward(fmt.Sprintf("%s:%s", ip, tcpPort)), // default ""
	}
	server, err := tftp.NewServer(":", opts...)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("ip %s , tcpport %s, port %s\n", ip, tcpPort, port)

	writeHandler := tftp.WriteHandlerFunc(writeTFTP)
	server.WriteHandler(writeHandler)

	log.Fatal(server.ListenAndServe())
}

// Write handler function
func writeTFTP(w tftp.WriteRequest) {
	log.Printf("Receive from %v", w.Addr())

	// Read the data from the client into memory
	data, err := ioutil.ReadAll(w)
	if err != nil {
		log.Fatalln(err)
		return
	}

	err = os.WriteFile(w.Name(), []byte(data), 0644)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("File %s written", w.Name())
}
