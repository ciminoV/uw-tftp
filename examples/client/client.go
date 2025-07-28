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

	// Configuring with a slice of options
	opts := []tftp.ClientOpt{
		tftp.ClientBlocksize(55),  // default 60
		tftp.ClientWindowsize(13), // default 1
		tftp.ClientTimeout(45),    // default 60
		tftp.ClientGuardTime(5),
		// tftp.ClientMode("NA"),     // default Octet
		// tftp.ClientTransferSize(true),
		// tftp.ClientRetransmit(3), // default 10
		// tftp.ClientTimeoutMultiplier(2),
	}

	filename := os.Args[1]
	if filename == "" {
		fmt.Print("Empty file name")
		os.Exit(1)
	}

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

	protocol := os.Args[2]
	if protocol == "tcp" {
		tcpIP := os.Args[3]
		tcpPort := os.Args[4]

		opts = append(opts, tftp.ClientTcpForward(fmt.Sprintf("%s:%s", tcpIP, tcpPort)))
		client, err := tftp.NewClient(opts...)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Println(client.Put(fmt.Sprintf("%s:%s/%s", "127.0.0.1", "69", filename), file, fileInfo.Size()))
	} else if protocol == "udp" {
		udpIP := os.Args[3]
		udpPort := os.Args[4]

		client, err := tftp.NewClient(opts...)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Println(client.Put(fmt.Sprintf("%s:%s/%s", udpIP, udpPort, filename), file, fileInfo.Size()))
	} else {
		fmt.Printf("%s is not a valid protocol (tcp/udp).\n", protocol)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	fmt.Printf("Elapsed time: %f\n", elapsed.Seconds())
	fmt.Printf("Elapsed time: %s\n", elapsed)
}
