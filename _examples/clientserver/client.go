package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"pack.ag/tftp"
)

func main() {
	start := time.Now()
	// in TCP mode I don't need server informations
	serverIp := os.Args[1]   // Server ip
	serverPort := os.Args[2] // UDP Server port
	clientIp := os.Args[3]   // Client ip
	clientPort := os.Args[4] // TCP Client port
	filename := os.Args[5]   // File to send

	// Get the file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// Get the file info so we can send size
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalln("error getting file size:", err)
	}

	// Configuring with a slice of options
	opts := []tftp.ClientOpt{
		tftp.ClientBlocksize(55),  // default 60
		tftp.ClientWindowsize(13), // default 1
		tftp.ClientTimeout(200),   // default 60
		// tftp.ClientRetransmit(3),  // default 5
		tftp.ClientTcpForward(fmt.Sprintf("%s:%s", clientIp, clientPort)), // default ""
	}
	client, err := tftp.NewClient(opts...)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("ip %s , port %s", clientIp, clientPort)
	log.Printf("ip %s , port %s", serverIp, serverPort)

	// Send file
	// this will return an error because we are not using a udp socket
	log.Println(client.Put(fmt.Sprintf("%s:%s/%s", serverIp, serverPort, filename), file, fileInfo.Size()))
	elapsed := time.Since(start)
	fmt.Printf("elapsed time %s", elapsed)
}
