package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// RespType represents the type of a RESP value.
type RespType int

const (
	RespTypeSimpleString RespType = iota
	RespTypeError
	RespTypeInteger
	RespTypeBulkString
	RespTypeArray
)

// RespValue represents a RESP value.
type RespValue struct {
	Type  RespType
	Value interface{}
}

// Encode encodes a Go value into a RESP value.
func Encode(v interface{}) (*RespValue, error) {
	switch v := v.(type) {
	case string:
		return &RespValue{RespTypeSimpleString, v}, nil
	case error:
		return &RespValue{RespTypeError, v.Error()}, nil
	case int:
		return &RespValue{RespTypeInteger, v}, nil
	case []byte:
		return &RespValue{RespTypeBulkString, v}, nil
	case []interface{}:
		var values []*RespValue
		for _, item := range v {
			val, err := Encode(item)
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		return &RespValue{RespTypeArray, values}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// Decode decodes a RESP value from a bufio.Reader.
func Decode(r *bufio.Reader) (*RespValue, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case '+':
		s, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return &RespValue{RespTypeSimpleString, s}, nil
	case '-':
		s, err := readLine(r)
		if err != nil {
			return nil, err
		}
		return &RespValue{RespTypeError, s}, nil
	case ':':
		n, err := readInt(r)
		if err != nil {
			return nil, err
		}
		return &RespValue{RespTypeInteger, n}, nil
	case '$':
		n, err := readInt(r)
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return &RespValue{RespTypeBulkString, nil}, nil
		}
		b := make([]byte, n)
		_, err = io.ReadFull(r, b)
		if err != nil {
			return nil, err
		}
		_, err = r.ReadByte() // read trailing \r\n
		if err != nil {
			return nil, err
		}
		return &RespValue{RespTypeBulkString, b}, nil
	case '*':
		n, err := readInt(r)
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return &RespValue{RespTypeArray, nil}, nil
		}
		var values []*RespValue
		for i := 0; i < n; i++ {
			v, err := Decode(r)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return &RespValue{RespTypeArray, values}, nil
	default:
		return nil, fmt.Errorf("unknown RESP type: %c", b)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("invalid line")
	}
	return string(line[:len(line)-2]), nil
}

func readInt(r *bufio.Reader) (int, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(line)
}

// Server implementation

type Server struct {
	listener net.Listener
	store    *Store
}

func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{
		listener: listener,
		store:    NewStore(),
	}, nil
}

func (s *Server) Start() error {
	fmt.Println("Redis server started, listening on", s.listener.Addr().String())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		resp, err := Decode(r)
		if err != nil {
			fmt.Println("error decoding RESP:", err)
			return
		}

		cmd, ok := resp.Value.([]interface{})
		if !ok {
			fmt.Println("invalid command format")
			return
		}

		respValue, err := s.processCommand(cmd)
		if err != nil {
			respValue = &RespValue{RespTypeError, err.Error()}
		}

		_, err = writeResponse(conn, respValue)
		if err != nil {
			fmt.Println("error writing response:", err)
			return
		}
	}
}

func writeResponse(conn net.Conn, respValue *RespValue) (int64, error) {
	var buf bytes.Buffer
	_, err := Encode(respValue).(io.WriterTo).WriteTo(&buf)
	if err != nil {
		return 0, err
	}
	return io.Copy(conn, &buf)
}

func (s *Server) processCommand(cmd []interface{}) (*RespValue, error) {
	switch strings.ToLower(cmd[0].(string)) {
	case "get":
		if len(cmd) != 2 {
			return &RespValue{RespTypeError, "wrong number of arguments for 'GET' command"}, nil
		}
		key, ok := cmd[1].(string)
		if !ok {
			return &RespValue{RespTypeError, "invalid key type"}, nil
		}
		value, err := s.store.Get(key)
		if err != nil {
			return &RespValue{RespTypeError, err.Error()}, nil
		}
		return &RespValue{RespTypeBulkString, value}, nil

	case "set":
		if len(cmd) != 3 {
			return &RespValue{RespTypeError, "wrong number of arguments for 'SET' command"}, nil
		}
		key, ok := cmd[1].(string)
		if !ok {
			return &RespValue{RespTypeError, "invalid key type"}, nil
		}
		value, ok := cmd[2].([]byte)
		if !ok {
			return &RespValue{RespTypeError, "invalid value type"}, nil
		}
		s.store.Set(key, value)
		return &RespValue{RespTypeSimpleString, "OK"}, nil

	default:
		return &RespValue{RespTypeError, "unknown command"}, nil
	}
}

// Store implementation
type Store struct {
	data map[string][]byte
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

func (s *Store) Get(key string) ([]byte, error) {
	value, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

func (s *Store) Set(key string, value []byte) {
	s.data[key] = value
}

func main() {
	server, err := NewServer(":6379")
	if err != nil {
		panic(err)
	}

	err = server.Start()
	if err != nil {
		panic(err)
	}
}
