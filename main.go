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
				shortName := strings.Split(entry.Name, ".")[0]
				peers[shortName] = PeerInfo{
					Name: shortName,
					IP:   entry.AddrV4.String(),
					Port: entry.Port,
				}
				mu.Unlock() // Unlock after writing the map
			} else {
				mu.Unlock() // Unlock map if we aleady know the peer.
			}
		}
	}()

	// start lookup
	// This tells the library: "Search for anyone using our protocol"
	mdns.Lookup("_p2p-mesh._tcp", entriesCh)

	// keep program alive
	fmt.Println("\n--- P2P Mesh Terminal ---")
	fmt.Println("Commands: 'list' (See peers), 'send [peer_name] [file_name]'")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		args := strings.Split(line, " ")
		switch args[0] {
		case "list":
			mu.Lock()
			fmt.Printf("\n --- Active Peers (%d) ---\n", len(peers))
			for name, info := range peers {
				fmt.Printf("- %s (%s:%d)\n", name, info.IP, info.Port)
			}
			mu.Unlock()
		case "send":
			if len(args) > 3 {
				fmt.Println("Usage: send [peer_name] [file_name]")
				continue
			}
			targetPeer := args[1]
			targetFile := args[2]
			go handleManualSend(targetPeer, targetFile)

		default:
			fmt.Println("Unkown command. Use 'list' or 'send'.")
		}
	}
}

func handleManualSend(targetPeer string, fileName string) {
	// a. look if the peer exists
	mu.Lock()
	peerInfo, exists := peers[targetPeer]
	mu.Unlock()
	if !exists {
		fmt.Printf("Error: Peer '%s' not found. Type 'list' to see active peers.\n", targetPeer)
		return
	}
	// b. open file before dialing
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error: Could not open file '%s': %v\n", fileName, err)
		return
	}
	defer file.Close()
	// c. get the total file size before sending.
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file status:", err)
		return
	}
	totalSize := fileInfo.Size()
	// d. dial the peer using ip and port in the map
	address := fmt.Sprintf("%s:%d", peerInfo.IP, peerInfo.Port)
	fmt.Printf("Dialing %s at %s... \n", targetPeer, address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error: Could not connect to %s: %v", targetPeer, err)
		return
	}
	defer conn.Close()
	// e. Send file name follwed by a newline(\n)
	fmt.Fprintln(conn, fileName)
	// New custom buffer streamignn loop replacing io.copy
	buffer := make([]byte, 32768)
	var totalBytesSent int64 = 0
	for {
		bytesRead, readErr := file.Read(buffer)
		if bytesRead > 0 {
			// write exactly the amount of bytes we read into the network connection
			_, writeErr := conn.Write(buffer[:bytesRead])
			if writeErr != nil {
				fmt.Printf("\nNetwork error during transfer: %v\n", writeErr)
				return
			}
			// update counter and calculate progress of file transfer
			totalBytesSent += int64(bytesRead)
			percentage := (float64(totalBytesSent) / float64(totalSize)) * 100
			fmt.Printf("\rProgress: %.2f%% (%d/%d bytes)", percentage, totalBytesSent, totalSize)
		}
		// check if we hit end of file
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			fmt.Printf("\nError reading file: %v\n", readErr)
			return
		}
	}
	fmt.Printf("[SUCCESS] send %s to %s!\n", fileName, targetPeer)
}
