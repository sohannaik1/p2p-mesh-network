package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/hashicorp/mdns"
)

type PeerInfo struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

var (
	peers = make(map[string]PeerInfo)
	mu    sync.Mutex

	namePtr *string
	portPtr *int
)

func main() {
	// Setup Flags
	namePtr = flag.String("name", "Sohan-Arch", "Name of the peer")
	portPtr = flag.Int("port", 9876, "Port to listen on")
	flag.Parse()

	// setup server
	service, _ := mdns.NewMDNSService(*namePtr, "_p2p-mesh._tcp", "", "", *portPtr, nil, []string{"Version=0.1"})
	server, _ := mdns.NewServer(&mdns.Config{Zone: service})
	defer server.Shutdown()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *portPtr))
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}

			reader := bufio.NewReader(conn)
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)

			newFile, err := os.Create("received_" + fileName)
			if err != nil {
				fmt.Println("File creation error:", err)
				conn.Close()
				continue
			}
			_, err = io.Copy(newFile, reader)
			if err == nil {
				fmt.Printf("[SUCCESS] Received file: %s from %s \n", fileName, conn.RemoteAddr())
			}
			newFile.Close()
			conn.Close()
		}
	}()

	fmt.Printf(">>> Node [%s] started on port %d <<<\n", *namePtr, *portPtr)

	// setup client
	// Create a channel to receive found entries
	entriesCh := make(chan *mdns.ServiceEntry, 10)

	// Start a goroutine to print whatever the channel finds
	go func() {
		for entry := range entriesCh {
			// a. Skip my own machine
			if strings.HasPrefix(entry.Name, *namePtr) {
				fmt.Println("Skipping myself")
				continue
			}
			// b. Lock the map until we write/check
			mu.Lock()
			_, alreadyExists := peers[entry.Name]

			if !alreadyExists {
				// c. The new entry is saved in the map if it already does not exist in map.
				peers[entry.Name] = PeerInfo{
					Name: entry.Name,
					IP:   entry.AddrV4.String(),
					Port: entry.Port,
				}
				mu.Unlock() // Unlock after writing the map

				// d. Dialing
				go func(e *mdns.ServiceEntry) {
					address := fmt.Sprintf("%s:%d", e.AddrV4, e.Port)
					conn, err := net.Dial("tcp", address)
					if err != nil {
						return
					}
					defer conn.Close()

					file, err := os.Open("secret.txt")
					if err != nil {
						fmt.Println("Error: Please create secret.txt file first")
						return
					}
					defer file.Close()

					fmt.Fprintln(conn, "secret.txt") // sends file name+\n
					_, err = io.Copy(conn, file)
					if err == nil {
						fmt.Printf(">>Sent secret.txt to %s\n", e.Name)
					}
				}(entry)
			} else {
				mu.Unlock() // Unlock map if we aleady know the peer.
			}
		}
	}()

	// start lookup
	// This tells the library: "Search for anyone using our protocol"
	mdns.Lookup("_p2p-mesh._tcp", entriesCh)

	// keep program alive
	select {}
}
