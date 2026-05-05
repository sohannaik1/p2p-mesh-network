package main

import (
	"bufio"
	"fmt"
	"net"

	"github.com/hashicorp/mdns"
)

func main() {
	// setup server
	service, _ := mdns.NewMDNSService("Sohan-Arch", "_p2p-mesh._tcp", "", "", 9876, nil, []string{"Version=0.1"})
	server, _ := mdns.NewServer(&mdns.Config{Zone: service})
	defer server.Shutdown()

	ln, err := net.Listen("tcp", ":9876")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("Error accepting connections. ", err)
				continue
			}
			fmt.Println("New peer connected from:", conn.RemoteAddr())
			message, _ := bufio.NewReader(conn).ReadString('\n')
			fmt.Print("Message received : ", message)
		}
	}()

	fmt.Println("mDNS Server started. Looking for peers...")

	// setup client
	// Create a channel to receive found entries
	entriesCh := make(chan *mdns.ServiceEntry, 10)

	// Start a goroutine to print whatever the channel finds
	go func() {
		for entry := range entriesCh {
			func(e *mdns.ServiceEntry) {
				address := fmt.Sprintf("%s:%d", e.AddrV4, e.Port)
				conn, err := net.Dial("tcp", address)
				if err != nil {
					return
				}
				defer conn.Close()
				fmt.Fprintf(conn, "Hello form Sohan-Arch\n")
			}(entry)
		}
	}()

	// start lookup
	// This tells the library: "Search for anyone using our protocol"
	mdns.Lookup("_p2p-mesh._tcp", entriesCh)

	// keep program alive
	select {}
}
