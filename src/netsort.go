package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

const (
	proto   = "tcp"
	clients = 3
)

var bidirectional_ch = make(chan []byte)

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)
	return scs
}

func listenForClientConnections(host string, port string) {
	serveraddr := host + ":" + port
	fmt.Println("Starting " + proto + " server on " + serveraddr)

	// Make the current server Listener
	listener, err := net.Listen(proto, serveraddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer listener.Close()

	// Accept all the connections coming to the Listener
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		// Go routine to handle all the client connections
		go handleClientConnections(conn)
	}
}

func handleClientConnections(conn net.Conn) {
	for {
		clientMessBuff := make([]byte, 101)
		// Read the data coming from the client connections (of size 101 bytes)
		bytes_read, err := conn.Read(clientMessBuff)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Send the message to the channel and break if first byte is 1
		if clientMessBuff[0] == byte(0) {
			message := clientMessBuff[:bytes_read]
			bidirectional_ch <- message
		} else {
			message := clientMessBuff[:bytes_read]
			bidirectional_ch <- message
			break
		}
	}
	conn.Close()
}

func sendData(connMap map[int]net.Conn, numServers int) {
	filePath := os.Args[2]
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer file.Close()

	// Number of bytes
	N := int(math.Log2(float64(numServers)))
	// Read 100 bytes continuously from the input file
	for {
		buf100 := make([]byte, 100)
		_, err := file.Read(buf100)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
		// get serverId from first N bytes of the data
		serverId := int(buf100[0] >> (8 - N))
		// Get Connection details from the map
		conn := connMap[serverId]
		// Write the data with the prefix of "0" if not EOF
		empty := []byte{0}
		mess := append(empty, buf100...)
		conn.Write(mess)
	}
	// Send a final entry with prefix "1" to all the connections indicating Stream_Complete
	for _, conn := range connMap {
		empty := []byte{1}
		token := make([]byte, 4)
		rand.Read(token)
		mess := append(empty, token...)
		conn.Write(mess)
	}
	// Error checking for closing file
	if err = file.Close(); err != nil {
		fmt.Printf("Could not close the file due to this %s error \n", err)
	}
}

func receiveDataFromClients(numServer int) [][]byte {
	records := [][]byte{}
	count := 0
	for {
		message := <-bidirectional_ch
		if message[0] == 1 {
			count++
		} else {
			records = append(records, message[1:])
		}
		if count == numServer {
			break
		}

	}
	return records
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	// Extract the current Host and Port
	host := ""
	port := ""
	for _, server := range scs.Servers {
		if server.ServerId == serverId {
			host = server.Host
			port = server.Port
		}
	}

	// Go routine to listen for connections
	go listenForClientConnections(host, port)

	// For all the clients
	connMap := make(map[int]net.Conn)
	// Create mesh of sockets
	for _, server := range scs.Servers {
		// if server.ServerId != serverId {
		for {
			serveraddr := server.Host + ":" + server.Port
			conn, err := net.Dial(proto, serveraddr)
			if err != nil {
				fmt.Println(err)
				continue
			}
			connMap[server.ServerId] = conn
			break
		}
		// }
	}

	fmt.Println("Mesh Completed")
	// Go routine to send data to all the connections

	numServers := len(connMap)
	go sendData(connMap, numServers)

	records := receiveDataFromClients(numServers)

	// Sort each file
	// keys := make([]string, 0, hmap)
	// for k := range hmap {
	// 	keys = append(keys, k)
	// }
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i][:11], records[j][:11]) < 0
	})

	writePath := os.Args[3]
	writeFile, err := os.Create(writePath)
	if err != nil {
		log.Println("Error opening writefile: ", err)
	}

	for _, k := range records {
		writeFile.Write(k)
	}

	writeFile.Close()

	log.Printf("Sorting %s to %s\n", os.Args[2], os.Args[3])
}
