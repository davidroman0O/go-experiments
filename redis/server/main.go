package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func main() {
	// Create a new TCP listener
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Redis-compatible server started. Listening on :6379")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		// Handle the connection in a new goroutine
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read and handle commands
	for {
		command, err := readCommand(conn)
		if err != nil {
			return
		}

		handleCommand(conn, command)
	}
}

func readCommand(conn net.Conn) ([]string, error) {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(string(buf[:n]), "\r\n")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid command")
	}

	commandType := parts[0][0]
	commandLength, _ := strconv.Atoi(parts[0][1:])

	if commandType != '*' || len(parts) < commandLength+1 {
		return nil, fmt.Errorf("invalid command")
	}

	command := make([]string, commandLength)
	for i := 0; i < commandLength; i++ {
		argType := parts[i+1][0]
		argLength, _ := strconv.Atoi(parts[i+1][1:])
		command[i] = parts[i+2][:argLength]
	}

	return command, nil
}

func handleCommand(conn net.Conn, command []string) {
	switch command[0] {
	case "GET":
		key := command[1]
		value, ok := data[key]
		if !ok {
			conn.Write([]byte("-ERR key not found\r\n"))
		} else {
			conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
		}
	case "SET":
		key := command[1]
		value := command[2]
		data[key] = value
		conn.Write([]byte("+OK\r\n"))
	default:
		// Handle custom commands here
		conn.Write([]byte("-ERR unknown command '" + command[0] + "'\r\n"))
	}
}

var data = make(map[string]string)
