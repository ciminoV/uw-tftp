package main

import (
	"fmt"
	"log"
	"os"

	"pack.ag/tftp"
)

func main() {
	localhost := "127.0.0.1" // Server ip
	port := os.Args[1]       // UDP Server port
	filename := os.Args[2]   // File to send

	// Get the file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// Get the file info so we can send size
	fileInfo, err := file.Stat()
	if err != nil {
		log.Println("error getting file size:", err)
	}

	// Configuring with a slice of options
	opts := []tftp.ClientOpt{
		tftp.ClientBlocksize(60),  // default 512
		tftp.ClientWindowsize(10), // default 1
		// tftp.ClientTimeout(25),    // default 20
		// tftp.ClientRetransmit(3),  // default 5
		tftp.ClientTcpForward(":55555"), // default ""
	}
	client, _ := tftp.NewClient(opts...)

	// Send file
	err = client.Put(fmt.Sprintf("%s:%s/%s", localhost, port, filename), file, fileInfo.Size())
	if err != nil {
		log.Fatalln(err)
	}
}
