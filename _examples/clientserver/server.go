package main

import (
	"io/ioutil"
	"log"
	"os"

	"pack.ag/tftp"
)

func main() {
	port := os.Args[1] // Server port
	opts := []tftp.ServerOpt{
		tftp.ServerRetransmit(5),        // default 5
		tftp.ServerSinglePort(true),     // default false
		tftp.ServerTcpForward(":55555"), // default ""
	}
	server, err := tftp.NewServer(":"+port, opts...)
	if err != nil {
		log.Fatal(err)
	}

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
