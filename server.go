package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var outgoingConnections map[net.Conn]string
var outgoingConnectionsInverse map[string]net.Conn

var clientConnections map[int64]net.Conn
var inverseClientConnections map[net.Conn]int64

var TransactionVotes map[int64]int64
var TransactionVotesLocks map[int64]*sync.Mutex

var wg sync.WaitGroup

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

var thisNode string

var numServers int

type TWEntry struct {
	TransactionID int64
	Account       string
	Value         int64
	Status        string
}

type RTSEntry struct {
	TransactionID int64
	Account       string //account it read
	Status        string
}

var TWLists map[string]([]TWEntry)
var RTSLists map[string]([]RTSEntry)
var TWLocks map[string]*sync.Mutex
var RTSLocks map[string]*sync.Mutex

var TWLock sync.Mutex
var RTSLock sync.Mutex

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

var configFileName string

func main() {

	//initialize priority queue

	argv := os.Args[1:]
	thisNode = argv[0]
	configFileName = argv[1]

	TWLists = make(map[string][]TWEntry)
	RTSLists = make(map[string][]RTSEntry)

	TWLocks = make(map[string]*sync.Mutex)
	RTSLocks = make(map[string]*sync.Mutex)

	configuration = make(map[string]string)
	inverse_configuration = make(map[string]string)

	parse(configFileName)

	outgoingConnections = make(map[net.Conn]string)
	outgoingConnectionsInverse = make(map[string]net.Conn)

	clientConnections = make(map[int64]net.Conn) //the key is the transactionID
	inverseClientConnections = make(map[net.Conn]int64)

	TransactionVotes = make(map[int64]int64)
	TransactionVotesLocks = make(map[int64]*sync.Mutex)

	go startServer()

	for _, v := range configuration {

		//wg.Add(1)
		go connectToNode(v)

	}

	//wg.Wait()
	for {

	}
}

// Function to start the server that listens for incoming connections.
func startServer() {
	//listeningPort := configuration[thisNode]
	portNum := ":" + (strings.Split(configuration[thisNode], ":"))[1]
	ln, err := net.Listen("tcp", portNum) // Listen on the specified port.
	if err != nil {
		//fmt.Println("Error starting server:", err)
		return
	}
	defer ln.Close() // Ensure the listener is closed when the function exits.

	for {
		conn, err := ln.Accept() // Accept new connections.
		if err != nil {
			//fmt.Println("Error accepting connection:", err)
			continue
		}
		// sender_address := fmt.Sprintf("%s:%s", conn.RemoteAddr().(*net.TCPAddr).IP, conn.RemoteAddr().(*net.TCPAddr).Port)
		// _, exists := inverse_configuration[sender_address]
		// if !exists {

		// 	go connectToNode(sender_address)
		// }

		go handleConnection(conn) // Handle the connection in a separate goroutine.
	}
}

// Function to handle incoming connections.
func handleConnection(conn net.Conn) {
	for {
		var decoder *gob.Decoder
		var incoming_message FinalGoal
		decoder = gob.NewDecoder(conn)
		decoder.Decode(&incoming_message)

		// if err != nil {
		// 	fmt.Println(err)
		// }
		if incoming_message.MessageType == "ClientToCoordinatorBegin" { //respond with ok
			//fmt.Println("Coordinator received begin")
			clientConnectionValue := connectToNode(incoming_message.ClientAddress)
			clientConnections[incoming_message.TransactionID] = clientConnectionValue
			inverseClientConnections[clientConnectionValue] = incoming_message.TransactionID
			TransactionVotes[incoming_message.TransactionID] = 0
			TransactionVotesLocks[incoming_message.TransactionID] = &sync.Mutex{}
			go Send(clientConnectionValue, FinalGoal{Message: "OK", MessageType: "CoordinatorToClientBeginAck"})

		}

		if incoming_message.MessageType == "ClientToCoordinatorOperation" {
			go CoordinatorOperation(incoming_message)

		}

		if incoming_message.MessageType == "ClientToCoordinatorCommit" {
			go CoordinatorCommit(incoming_message, conn)

		}

		if incoming_message.MessageType == "ClientToCoordinatorAbort" {
			go CoordinatorAbortFromClient(incoming_message, conn)
			go Send(clientConnections[incoming_message.TransactionID], FinalGoal{Message: "ABORTED", MessageType: "CoordinatorToClientAbort"})

		}

		if incoming_message.MessageType == "CoordinatorToServersOperation" {
			go ServerOperation(incoming_message, conn)

		}

		if incoming_message.MessageType == "CoordinatorToServersAbort" {
			go ServerAbort(incoming_message)

		}

		if incoming_message.MessageType == "CoordinatorToServersCommit" {
			go ServerCommit(conn, incoming_message)

		}

		if incoming_message.MessageType == "CoordinatorToServersVote" {
			go ServerVote(incoming_message, conn)

		}

		if incoming_message.MessageType == "ServersToCoordinatorsAbort" {
			go CoordinatorAbortFromServer(incoming_message)

		}

		if incoming_message.MessageType == "ServersToCoordinatorsVote" {
			if incoming_message.Vote == "YES" {
				TransactionVotesLocks[incoming_message.TransactionID].Lock()
				TransactionVotes[incoming_message.TransactionID]++
				//check if required number of votes has been received
				//fmt.Println("Num Servers: \n", numServers)
				//fmt.Printf("Transaction: %d Votes: %d\n", incoming_message.TransactionID, TransactionVotes[incoming_message.TransactionID])
				if TransactionVotes[incoming_message.TransactionID] >= int64(numServers) {
					for _, connection := range outgoingConnectionsInverse {
						//fmt.Printf("Sending commit message to servers for transaction %d \n", incoming_message.TransactionID)
						go Send(connection, FinalGoal{TransactionID: incoming_message.TransactionID, MessageType: "CoordinatorToServersCommit", CoordinatorSender: thisNode})
					}
					//also tell client that its transaction has been committed
					//fmt.Printf("Sending commit message to client for transaction %d \n", incoming_message.TransactionID)
					go Send(clientConnections[incoming_message.TransactionID], FinalGoal{Message: "COMMIT OK", MessageType: "CoordinatorToClientCommitAck"})
				}
				TransactionVotesLocks[incoming_message.TransactionID].Unlock()
			} else {
				//tell all servers to abort, don't need to bother removing transactionID from TransactionVotes map (assume never any duplicate transaction IDs even from same client)
				for _, connection := range outgoingConnectionsInverse {
					go Send(connection, FinalGoal{TransactionID: incoming_message.TransactionID, MessageType: "CoordinatorToServersAbort", CoordinatorSender: thisNode})
				}
				//also tell client that it has been aborted
				go Send(clientConnections[incoming_message.TransactionID], FinalGoal{Message: "ABORTED", MessageType: "CoordinatorToClientAbort"})

			}

		}

		if incoming_message.MessageType == "ServersToCoordinatorOperationAck" {

			go CoordinatorHandlesOperationResponse(incoming_message)

		}

		if incoming_message.MessageType == "ServerToCoordinatorCommitAccountInfo" {
			go Send(clientConnections[incoming_message.TransactionID], FinalGoal{TransactionID: incoming_message.TransactionID, Account: incoming_message.Account, Value: incoming_message.Value, MessageType: "CoordinatorToClientCommitAccountInfo"})

		}

	}

}

// Function to establish a connection to another node.
func connectToNode(address string) (result net.Conn) {
	_, exists := inverse_configuration[address]
	if exists {
		//defer wg.Done()
	}
	conn, err := net.Dial("tcp", address) // Dial the specified address.
	if err != nil {
		//fmt.Println("Error connecting to node:", err)
		if exists {
			//wg.Add(1)
		}
		go connectToNode(address)
		return
	}

	branchName, exists := inverse_configuration[address]
	if exists {

		outgoingConnections[conn] = branchName
		outgoingConnectionsInverse[branchName] = conn

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

func ServerOperation(mes FinalGoal, conn net.Conn) {

	conn = outgoingConnectionsInverse[mes.CoordinatorSender]
	//should update the data, and then relay a message back to the coordinator - can be a successful message or an abort.
	//3 types of operations - balance, withdraw, deposit. all three require reads first. Withdraw and deposit require writes
	//fmt.Println("DOING OPERATION")
	message_contents := strings.Split(mes.Message, " ")
	operation := message_contents[0]
	account := message_contents[1]

	//read the account - if it does not exist and operation is not DEPOSIT, return failure to the coordinator
	//TWLock.Lock()
	//fmt.Printf("%d STUCK IN OPERATION AT BEGINNING\n", mes.TransactionID)
	_, exists := TWLists[account]
	if !exists && operation != "DEPOSIT" {
		//send abort to coordinator
		//TWLock.Unlock()
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Message: "NOT FOUND, ABORTED", MessageType: "ServersToCoordinatorsAbort"})
		return
	}
	//otherwise add it to the TW list if it does not exist
	if !exists {
		TWLists[account] = []TWEntry{}
		TWLocks[account] = &sync.Mutex{}
	}
	//TWLock.Unlock()

	//also want to add it to RTS lists if it does not exist
	if !exists {
		//RTSLock.Lock()
		RTSLists[account] = []RTSEntry{}
		RTSLocks[account] = &sync.Mutex{}
		//RTSLock.Unlock()

	}

	//now perform the read
	//TWLocks[account].Lock()
	var tempValue int64
	tempValue = 0
	var maxTID int64
	maxTID = -1
	//entries := TWLists[account]
	validRead := true
	//check if transaction is greater than write timestamp on committed version of object
	for i := 0; i < len(TWLists[account]); i++ {
		if TWLists[account][i].Status == "COMMITTED" && TWLists[account][i].TransactionID >= mes.TransactionID {
			validRead = false
			break
		}
	}
	if validRead == false { //should abort
		//TWLocks[account].Unlock()
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Message: "ABORTED", MessageType: "ServersToCoordinatorsAbort"})
		return

	}

	//find DS - find its index
	DSIndex := -1
	for i := 0; i < len(TWLists[account]); i++ {
		if TWLists[account][i].TransactionID >= maxTID && TWLists[account][i].TransactionID <= mes.TransactionID && TWLists[account][i].Status != "ABORTED" && TWLists[account][i].Status != "IGNORE" {
			DSIndex = i
			maxTID = TWLists[account][i].TransactionID
		}
	}

	//if you've found a value, need to do some more stuff - otherwise, can simply proceed
	//fmt.Println("Entries: \n", entries)
	if DSIndex != -1 { //found a value, otherwise, proceed with a DEPOSIT
		if TWLists[account][DSIndex].Status == "COMMITTED" {
			tempValue = TWLists[account][DSIndex].Value
			//add transaction ID to RTS list
			//RTSLocks[account].Lock()
			already_there := false
			//RTSEntries := RTSLists[account]
			for j := 0; j < len(RTSLists[account]); j++ {
				if RTSLists[account][j].TransactionID == mes.TransactionID && RTSLists[account][j].Status != "IGNORE" {
					already_there = true
				}
			}
			if !already_there {
				RTSLists[account] = append(RTSLists[account], RTSEntry{TransactionID: mes.TransactionID, Account: account})
				//RTSLists[account] = RTSEntries
			}
			//RTSLocks[account].Unlock()

		} else if TWLists[account][DSIndex].TransactionID == mes.TransactionID {
			tempValue = TWLists[account][DSIndex].Value

		} else {

			//TWLocks[account].Unlock()

			//wait until transaction that wrote DS is committed or aborted - when it is, reapply read rule
			//to tell if its aborted, store the current transaction ID of DS, so you can check if it's still in the list
			// dsID := entries[DSIndex].TransactionID
			// //have one second timeout in case something goes wrong

			// abortedOrCommitted := false
			// for !abortedOrCommitted {
			// 	fmt.Println("Operation Loop Entries: \n", entries)
			// 	notThere := true
			// 	for s := 0; s < len(entries); s++ {
			// 		if entries[s].TransactionID == dsID {
			// 			notThere = false
			// 		}
			// 		if entries[s].TransactionID == dsID && entries[s].Status == "COMMITTED" {
			// 			abortedOrCommitted = true
			// 		}

			// 	}
			// 	if notThere {
			// 		abortedOrCommitted = true
			// 	}

			// }
			go ServerOperation(mes, conn)
			return

		}

	} else if operation != "DEPOSIT" {
		go ServerOperation(mes, conn)
		return

	}

	//fmt.Printf("Read Value: %d\n", tempValue)
	if operation == "BALANCE" { //simply send the read value back to the coordinator
		//TWLocks[account].Unlock()
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Account: account, Value: tempValue, Success: "SUCCESS", MessageType: "ServersToCoordinatorOperationAck"})
		return
	}

	//at this point we have the read value - if we've read nothing because the TW list is empty, tempValue is 0
	var writeValue int64
	argument, _ := strconv.ParseInt(message_contents[2], 10, 64)
	if operation == "WITHDRAW" {
		writeValue = tempValue - argument
	} else if operation == "DEPOSIT" {
		writeValue = tempValue + argument
	}

	//now want to write back into entries
	//need to make sure transaction ID is greater than or equal to max read timestamp on object, and greater than write timestamp on committed version of D
	validWrite := true
	//RTSLocks[account].Lock()
	//RTSEntries := RTSLists[account]
	for k := 0; k < len(RTSLists[account]); k++ {
		if RTSLists[account][k].TransactionID > mes.TransactionID && RTSLists[account][k].Status != "ABORTED" && RTSLists[account][k].Status != "IGNORE" {
			validWrite = false
			break

		}
	}
	//RTSLocks[account].Unlock()

	for l := 0; l < len(TWLists[account]); l++ {
		if TWLists[account][l].Status == "COMMITTED" && TWLists[account][l].TransactionID >= mes.TransactionID {
			validWrite = false
			break
		}
	}
	if validWrite == false {
		//RTSLocks[account].Unlock()
		//TWLocks[account].Unlock()
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Message: "ABORTED", MessageType: "ServersToCoordinatorsAbort"})
		return
	}

	//now perform the tentative write
	//first check to see if there's already an entry (not committed) in the TW list
	TWEntryIndex := -1
	for m := 0; m < len(TWLists[account]); m++ {
		if TWLists[account][m].TransactionID == mes.TransactionID && TWLists[account][m].Status == "TENTATIVE" {
			TWEntryIndex = m
			break
		}
	}
	if TWEntryIndex != -1 {
		TWLists[account][TWEntryIndex].Value = writeValue
	} else {
		TWLists[account] = append(TWLists[account], TWEntry{TransactionID: mes.TransactionID, Account: account, Value: writeValue, Status: "TENTATIVE"})

	}
	//TWLists[account] = entries
	//fmt.Println("Operation entries: ", entries)
	//if withdraw or update still need to send acknowledgement to coordinator, but say nothing to report
	//fmt.Println("Sending Some Operation Acknowledgement Back, Transaction ID\n", mes.TransactionID)
	//fmt.Printf("%d sending acknowledgement\n", mes.TransactionID)
	go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Account: account, Value: writeValue, Success: "SUCCESS BUT DON'T REPORT", MessageType: "ServersToCoordinatorOperationAck"})

	//TWLocks[account].Unlock()

}

func ServerCommit(conn net.Conn, mes FinalGoal) { //at this point coordinator has told server to commit
	conn = outgoingConnectionsInverse[mes.CoordinatorSender]
	//need to wait until all transactions with lower timestamp are committed or aborted
	//need to update the TWList, not the RTS list - you update both when it comes to abort
	//just go through every object with and try and commit appropriate transactionIDs
	//fmt.Printf("%d trying to commit\n", mes.TransactionID)
	//go through each account lock, and use it on the account - if can't commit yet, call function again
	for account, _ := range TWLists { //replace _ with lock
		//lock.Lock()

		beenTouched := false
		for i := 0; i < len(TWLists[account]); i++ {
			if TWLists[account][i].TransactionID == mes.TransactionID && TWLists[account][i].Status == "TENTATIVE" {
				beenTouched = true
			}
		}
		if !beenTouched {
			continue
		}

		//TWAccountList := TWLists[account]
		//fmt.Println("Commit Loop Entries: \n", TWAccountList)
		//check to see if lower uncommitted transaction IDs exist
		lowerUncommittedExists := false
		for i := 0; i < len(TWLists[account]); i++ {
			if TWLists[account][i].TransactionID < mes.TransactionID && TWLists[account][i].Status == "TENTATIVE" {
				lowerUncommittedExists = true

			}
		}
		if lowerUncommittedExists {
			//lock.Unlock()
			//fmt.Print("Bootloop\n", mes.TransactionID)

			ServerCommit(conn, mes) //try again
			return

		}
		//otherwise, update committed value and remove corresponding TW entry
		//get the TW write value, and delete the TW entry from the list
		var valueToBeCommitted int64
		relevantAccount := false
		for i := 0; i < len(TWLists[account]); i++ {
			if TWLists[account][i].TransactionID == mes.TransactionID && TWLists[account][i].Status == "TENTATIVE" {
				valueToBeCommitted = TWLists[account][i].Value
				//remove it from list
				//TWLists[account] = append(TWLists[account][:i], TWLists[account][i+1:]...)
				TWLists[account][i].Status = "IGNORE"
				relevantAccount = true
				//TWLists[account] = TWAccountList

			}
		}

		//check to see if committed value exists
		if relevantAccount {
			committedIndex := -1
			for i := 0; i < len(TWLists[account]); i++ {
				//fmt.Print("Commit Success\n", mes.TransactionID)
				if TWLists[account][i].Status == "COMMITTED" {
					committedIndex = i

				}
			}

			if committedIndex != -1 { //just update the currently committed object
				//fmt.Print("Commit Success\n", mes.TransactionID)
				TWLists[account][committedIndex].Status = "COMMITTED"
				TWLists[account][committedIndex].TransactionID = mes.TransactionID
				TWLists[account][committedIndex].Value = valueToBeCommitted

			} else { //need to add a new committed object
				//fmt.Print("Commit Success\n", mes.TransactionID)
				TWLists[account] = append(TWLists[account], TWEntry{TransactionID: mes.TransactionID, Account: account, Value: valueToBeCommitted, Status: "COMMITTED"})

			}
			//fmt.Printf("Entries and I'm inside commit function that %d is using, %T\n", mes.TransactionID, TWLists[account])
			//TWLists[account] = TWAccountList
			//fmt.Printf("Value to Be Committed: %d\n", valueToBeCommitted)
			go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Account: account, Value: valueToBeCommitted, MessageType: "ServerToCoordinatorCommitAccountInfo"})
			fmt.Printf("%s = %d\n", account, valueToBeCommitted)
		}

		//lock.Unlock()
	}

}

func ServerAbort(mes FinalGoal) {
	//get rid of all entries with corresponding transaction ID in TWlists (there should only be "TENTATIVE" entries for the transaction) and in RTS lists
	//in TWLists, if list has become empty, need to delete it from map, and need to delete the corresponding RTS list too for the account
	for account, _ := range TWLocks { //replace _ with lock
		//lock.Lock()

		//TWAccountList := TWLists[account]
		//fmt.Println("Abort entries", TWAccountList)
		for i := 0; i < len(TWLists[account]); i++ {
			if TWLists[account][i].TransactionID == mes.TransactionID { //MIGHT BE A SOURCE OF ERROR HERE
				//TWLists[account] = append(TWLists[account][:i], TWLists[account][i+1:]...)
				TWLists[account][i].Status = "ABORTED"
				//fmt.Printf("Successfully aborted %d\n", mes.TransactionID)
				//TWLists[account] = TWAccountList
				//i--
			}
		}

		//check if everything is 0
		var lengthZero = true
		for i := 0; i < len(TWLists[account]); i++ {
			if TWLists[account][i].Status != "ABORTED" {
				lengthZero = false

			}
		}
		if lengthZero {
			//remove it from all maps
			lengthZero = true
			delete(TWLists, account)
			delete(RTSLists, account)
			delete(RTSLocks, account)
			//lock.Unlock()
			delete(TWLocks, account)

		}

		//also need to remove it from RTS lists
		if !lengthZero {
			//RTSLocks[account].Lock()
			//ReadList := RTSLists[account]
			for i := 0; i < len(RTSLists[account]); i++ {
				if RTSLists[account][i].TransactionID == mes.TransactionID { //MIGHT BE A SOURCE OF ERROR HERE
					RTSLists[account][i].Status = "ABORTED"
					//RTSLists[account] = ReadList
					//i--
				}
			}
			//RTSLocks[account].Unlock()
		}

		if !lengthZero {
			//lock.Unlock()
		}
	}

}

func ServerVote(mes FinalGoal, conn net.Conn) {
	conn = outgoingConnectionsInverse[mes.CoordinatorSender]
	//just need to check if all TW entries corresponding to a value to be written are positive
	canCommit := true
	for account, _ := range TWLocks { //replace _ with lock
		//lock.Lock()
		TWAccountList := TWLists[account]
		for i := 0; i < len(TWAccountList); i++ {
			if TWAccountList[i].TransactionID == mes.TransactionID && TWAccountList[i].Status == "TENTATIVE" && TWAccountList[i].Value < 0 {
				canCommit = false

			}
		}
		//lock.Unlock()
	}
	//if canCommit, send positive vote to server - otherwise send negative vote - use vote struct
	//fmt.Printf("CAN I COMMIT? %t\n", canCommit)
	if canCommit {
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Vote: "YES", MessageType: "ServersToCoordinatorsVote"})
	} else {
		go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Vote: "NO", MessageType: "ServersToCoordinatorsVote"})

	}

}

func CoordinatorCommit(mes FinalGoal, conn net.Conn) { //ask for votes, wait for votes, once received tell all to commit, and tell client
	//send a vote to all servers
	//fmt.Println("COORDINATOR COMMITTING")
	for _, connection := range outgoingConnectionsInverse {
		go Send(connection, FinalGoal{TransactionID: mes.TransactionID, MessageType: "CoordinatorToServersVote", CoordinatorSender: thisNode})
	}
	//handle the vote responses in the handle function

}

func CoordinatorAbortFromClient(mes FinalGoal, conn net.Conn) { //tell all servers to abort
	for _, connection := range outgoingConnectionsInverse {
		go Send(connection, FinalGoal{TransactionID: mes.TransactionID, MessageType: "CoordinatorToServersAbort", CoordinatorSender: thisNode})
	}

}

func CoordinatorAbortFromServer(mes FinalGoal) { //tell all servers to abort, and tell client
	for _, connection := range outgoingConnectionsInverse {
		go Send(connection, FinalGoal{TransactionID: mes.TransactionID, MessageType: "CoordinatorToServersAbort", CoordinatorSender: thisNode})
	}

	//inform client
	go Send(clientConnections[mes.TransactionID], FinalGoal{Message: mes.Message, MessageType: "CoordinatorToClientAbort"})

}

func CoordinatorOperation(mes FinalGoal) {
	//figure out which server to send itto
	account_branch := strings.Split(mes.Message, " ")[1]
	branch := strings.Split(account_branch, ".")[0]
	//fmt.Printf("%s     %s\n", branch, outgoingConnectionsInverse[branch])
	conn := outgoingConnectionsInverse[branch]
	go Send(conn, FinalGoal{TransactionID: mes.TransactionID, Message: mes.Message, MessageType: "CoordinatorToServersOperation", CoordinatorSender: thisNode})

	//go Send(conn, CoordinatorToServersOperation{TransactionID: mes.TransactionID, Message: mes.Message, IgnoreNine: -1})

}

func CoordinatorHandlesOperationResponse(mes FinalGoal) {
	//types of responses: success, success but don't report - can relay these back to the client directly
	//aborted and not found, aborted are in different structs
	go Send(clientConnections[mes.TransactionID], FinalGoal{Account: mes.Account, Value: mes.Value, Success: mes.Success, MessageType: "CoordinatorToClientOperationAck"})

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

		//nodes[split[0]] = Parse{ServerName: split[0], HostID: split[1], port: split[2]}
		configuration[split[0]] = split[1] + ":" + split[2]
		inverse_configuration[split[1]+":"+split[2]] = split[0]
		// example nodes[node1] = NodeNum: node1, HostID: sp24-cs425-0601.cs.illinois.edu, port: 1234

	}

}
