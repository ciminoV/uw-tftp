package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"pack.ag/tftp"
)

func main() {
	var server *tftp.Server
	var err error

	opts := []tftp.ServerOpt{
		// tftp.ServerRetransmit(5),
		tftp.ServerSinglePort(true),
	}

	protocol := os.Args[1]
	if protocol == "tcp" {
		ip := os.Args[2]
		port := os.Args[3]

		opts = append(opts, tftp.ServerTcpForward(fmt.Sprintf("%s:%s", ip, port)))
		server, err = tftp.NewServer(":", opts...)
		if err != nil {
			log.Fatal(err)
		}
	} else if protocol == "udp" {
		port := os.Args[2]

		server, err = tftp.NewServer(":"+port, opts...)

		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Printf("%s is not a valid protocol (tcp/udp).\n", protocol)
		os.Exit(1)
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
