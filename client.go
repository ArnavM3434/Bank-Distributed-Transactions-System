package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var configuration map[string]string

// = map[string]string{
// 	"A": "sp24-cs425-0601.cs.illinois.edu:1234",
// 	"B": "sp24-cs425-0602.cs.illinois.edu:1234",
// 	"C": "sp24-cs425-0603.cs.illinois.edu:1234",
// 	"D": "sp24-cs425-0604.cs.illinois.edu:1234",
// 	"E": "sp24-cs425-0605.cs.illinois.edu:1234",
// }

var inverse_configuration map[string]string

// = map[string]string{
// 	"sp24-cs425-0601.cs.illinois.edu:1234": "A",
// 	"sp24-cs425-0602.cs.illinois.edu:1234": "B",
// 	"sp24-cs425-0603.cs.illinois.edu:1234": "C",
// 	"sp24-cs425-0604.cs.illinois.edu:1234": "D",
// 	"sp24-cs425-0605.cs.illinois.edu:1234": "E",
// }

var branches []string
var receivedAbort bool

type FinalGoal struct {
	MessageType       string
	TransactionID     int64
	Message           string
	Account           string
	Value             int64
	Success           string
	Vote              string
	CoordinatorSender string
	ClientAddress     string
}

var clientId string

var tId int64

var coordinator string

var coordinatorConnection net.Conn

var affectedAccounts map[string]int64

var accountReceivedLock sync.Mutex
var accountsReceived int

var beenCommited bool
var beenCommittedMessage string

var myAddress string

var configFileName string

var responseReceived bool

var numServers int

func main() {

	//initialize priority queue
	receivedAbort = false
	argv := os.Args[1:]
	clientId = argv[0]
	configFileName = argv[1]
	//fmt.Println(inputFileName)
	configuration = make(map[string]string)
	inverse_configuration = make(map[string]string)
	parse(configFileName)
	//fmt.Println(numServers)
	go startServer()

	responseReceived = true

	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	randomnum := random.Intn(5)

	coordinator = branches[randomnum%5]

	affectedAccounts = make(map[string]int64)

	connectToNode(configuration[coordinator])

	reader := bufio.NewReader(os.Stdin)
	for {

		if responseReceived && receivedAbort == false {
			message, _ := reader.ReadString('\n')
			//fmt.Println(message)
			message = strings.TrimSpace(message)
			if message == "BEGIN" {
				tId = time.Now().UnixNano()
				//fmt.Printf("%d\n", tId)
				accountsReceived = 0
				beenCommited = false
				affectedAccounts = make(map[string]int64)
				go Send(coordinatorConnection, FinalGoal{TransactionID: tId, MessageType: "ClientToCoordinatorBegin", ClientAddress: myAddress})
				responseReceived = false
			} else if message == "COMMIT" {
				//fmt.Println("COMMITTING CLIENT SIDE")
				go Send(coordinatorConnection, FinalGoal{TransactionID: tId, MessageType: "ClientToCoordinatorCommit"})
				responseReceived = false
			} else if message == "ABORT" {
				go Send(coordinatorConnection, FinalGoal{TransactionID: tId, MessageType: "ClientToCoordinatorAbort"})
				responseReceived = false
			} else {
				//relay operation to coordinator
				//find which account is being affected, add it to the map - only do if deposit or withdraw operation
				if len(message) > 1 {
					message_contents := strings.Split(message, " ")
					operation := message_contents[0]
					account := message_contents[1]
					if operation == "DEPOSIT" || operation == "WITHDRAW" {

					} else {
						affectedAccounts[account] = 0
					}
					go Send(coordinatorConnection, FinalGoal{TransactionID: tId, Message: message, MessageType: "ClientToCoordinatorOperation"})
					responseReceived = false
				}
			}

		}

	}

}

// Function to start the server that listens for incoming connections.
func startServer() {
	//listeningPort := configuration[thisNode]
	var portNum string
	if clientId == "a" {
		portNum = ":20001"

	} else if clientId == "b" {
		portNum = ":20002"

	} else if clientId == "c" {
		portNum = ":20003"

	} else if clientId == "d" {
		portNum = ":20004"

	} else if clientId == "e" {
		portNum = ":20005"

	} else {
		portNum = ":1234"
	}
	ln, err := net.Listen("tcp", portNum) // Listen on the specified port.
	hostname, _ := os.Hostname()
	myAddress = hostname + portNum
	//myAddress = "localhost:20001"
	//fmt.Printf("Address: %s\n", myAddress)

	if err != nil {
		//fmt.Println("Error starting server:", err)
		return
	}
	defer ln.Close() // Ensure the listener is closed when the function exits.
	//fmt.Println("Server started on port", ":1234")

	for {
		conn, err := ln.Accept() // Accept new connections.
		if err != nil {
			//fmt.Println("Error accepting connection:", err)
			continue
		}
		//coordinatorConnection = conn
		go handleConnection(conn) // Handle the connection in a separate goroutine.
	}
}

// Function to handle incoming connections.
func handleConnection(conn net.Conn) {

	for {
		//fmt.Println("YO I GOT SOMETHING")
		var decoder *gob.Decoder
		//var err error
		var incoming_message FinalGoal
		decoder = gob.NewDecoder(conn)
		decoder.Decode(&incoming_message)

		// if err != nil {
		// 	fmt.Println(err)
		// }
		if incoming_message.MessageType == "CoordinatorToClientBeginAck" { //respond with ok
			//fmt.Println("RECEPTION - begin ack")
			fmt.Println("OK")
			responseReceived = true
			//continue
		}

		if incoming_message.MessageType == "CoordinatorToClientOperationAck" { //respond with ok
			//fmt.Println("RECEPTION - operation ack")
			if incoming_message.Success == "SUCCESS" {
				fmt.Printf("%s = %d\n", incoming_message.Account, incoming_message.Value)
				affectedAccounts[incoming_message.Account] = incoming_message.Value
			} else {
				//affectedAccounts[incoming_message.Account] = incoming_message.Value
				fmt.Println("OK")
			}
			responseReceived = true

			//continue
		}

		if incoming_message.MessageType == "CoordinatorToClientAbort" { //respond with ok
			//fmt.Println("RECEPTION - abort")
			fmt.Printf("%s\n", incoming_message.Message)
			responseReceived = true
			receivedAbort = true
			//continue
		}

		if incoming_message.MessageType == "CoordinatorToClientCommitAck" { //respond with ok
			//fmt.Println("RECEPTION - commit ack")
			beenCommited = true
			//fmt.Printf("%s\n", incoming_message4.Message)
			beenCommittedMessage = incoming_message.Message

			//print out all the accounts info
			//fmt.Println("Number of Commit Accounts Received: \n", accountsReceived)
			// for key, value := range affectedAccounts {
			// 	if value > 0 {
			// 		fmt.Printf("%s = %d\n", key, value)
			// 	}
			// }
			//fmt.Printf("\n")
			fmt.Printf("COMMIT OK\n")
			responseReceived = true

			//continue
		}

		if incoming_message.MessageType == "CoordinatorToClientCommitAccountInfo" { //respond with ok
			//fmt.Println("RECEPTION - commitaccountinfo ack")
			affectedAccounts[incoming_message.Account] = incoming_message.Value
			//accountReceivedLock.Lock()
			accountsReceived++
			//accountReceivedLock.Unlock()

			//continue
		}

	}

}

// Function to establish a connection to another node.
func connectToNode(address string) (result net.Conn) {

	conn, err := net.Dial("tcp", address) // Dial the specified address.
	coordinatorConnection = conn
	if err != nil {
		//fmt.Println("Error connecting to node:", err)

		go connectToNode(address)
		return
	}

	return conn

}

func Send(conn net.Conn, data interface{}) error {
	// Create an encoder writing to the network connection
	encoder := gob.NewEncoder(conn)
	//fmt.Println("SENDING TO CLIENT")
	// Encode the value
	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}

func parse(filePath string) {
	file, ferr := os.Open(filePath)

	if ferr != nil {
		panic(ferr) // terminates the program
	}

	scanner := bufio.NewScanner(file) // creates a new scanner object that reads from fil
	//scanner.Scan()                    // Skip the first line, reads the next token

	for scanner.Scan() {
		line := scanner.Text() // retrives the current line as a string

		split := strings.Fields(line) //split the line into fields using whitespace

		if len(split) != 3 {
			continue
		}
		numServers++

		branches = append(branches, split[0])

		//nodes[split[0]] = Parse{ServerName: split[0], HostID: split[1], port: split[2]}
		configuration[split[0]] = split[1] + ":" + split[2]
		inverse_configuration[split[1]+":"+split[2]] = split[0]
		// example nodes[node1] = NodeNum: node1, HostID: sp24-cs425-0601.cs.illinois.edu, port: 1234

	}

}
