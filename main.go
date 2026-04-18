package main

import (
	"fmt"

	"github.com/hashicorp/mdns"
)

func main() {
	// 1. Setup the Server (Your existing code)
	service, _ := mdns.NewMDNSService("Sohan-Arch", "_p2p-mesh._tcp", "", "", 9876, nil, []string{"Version=0.1"})
	server, _ := mdns.NewServer(&mdns.Config{Zone: service})
	defer server.Shutdown()

	fmt.Println("mDNS Server started. Looking for peers...")

	// 2. Setup the Client (The Discovery part)
	// Create a channel to receive found entries
	entriesCh := make(chan *mdns.ServiceEntry, 10)

	// Start a goroutine to print whatever the channel finds
	go func() {
		for entry := range entriesCh {
			fmt.Printf("Found Peer! Name: %s | IP: %v | Port: %d\n", entry.Name, entry.AddrV4, entry.Port)
		}
	}()

	// 3. Start the Lookup
	// This tells the library: "Search for anyone using our protocol"
	mdns.Lookup("_p2p-mesh._tcp", entriesCh)

	// Keep the program alive
	select {}
}
