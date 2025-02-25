package main

import (
	"fmt"
	"log"
	"os"

	"pack.ag/tftp"
)

func main() {
	ip := os.Args[1]       // Server ip
	port := os.Args[2]     // Server port
	filename := os.Args[3] // File to send

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
		tftp.ClientTimeout(15),    // default 20
		tftp.ClientRetransmit(3),  // default 5
	}
	client, _ := tftp.NewClient(opts...)

	// Send file
	err = client.Put(fmt.Sprintf("%s:%s/%s", localhost, port, filename), file, fileInfo.Size())
	if err != nil {
		log.Fatalln(err)
	}
}
