package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log/slog"
	"net/http"

	"github.com/bytecodealliance/wasmtime-go/v18"
)

var storeModule *wasmtime.Module
var actorModule *wasmtime.Module
var store *wasmtime.Store

func init() {
	engine := wasmtime.NewEngine()
	store = wasmtime.NewStore(engine)
	var err error

	// Load store module
	storeModule, err = loadModule(store, "./store.wasm")
	if err != nil {
		panic(err)
	}

	// Load actor module
	actorModule, err = loadModule(store, "./actor.wasm")
	if err != nil {
		panic(err)
	}
	slog.Info("loaded")
}

func main() {
	http.HandleFunc("/process", processHandler)
	if err := http.ListenAndServe(":4201", nil); err != nil {
		panic(err)
	}
}

func processHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	result, err := processMessages(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(result)
}

func processMessages(messages []byte) ([]byte, error) {

	// Instantiate modules and establish bindings
	storeInstance, err := wasmtime.NewInstance(store, storeModule, []wasmtime.AsExtern{})
	if err != nil {
		return nil, err
	}

	actorInstance, err := wasmtime.NewInstance(store, actorModule, []wasmtime.AsExtern{})
	if err != nil {
		return nil, err
	}

	// Assuming 'register_actor' and 'process_messages' are exported functions in the store module
	// and 'process_message' is an exported function in the actor module.
	// Bind actor to store
	registerActorFunc := storeInstance.GetFunc(store, "register_actor")
	if registerActorFunc == nil {
		return nil, fmt.Errorf("register_actor function not found")
	}

	_, err = registerActorFunc.Call(store, actorInstance)
	if err != nil {
		return nil, err
	}

	// Process messages
	processMessagesFunc := storeInstance.GetFunc(store, "process_messages")
	if processMessagesFunc == nil {
		return nil, fmt.Errorf("process_messages function not found")
	}

	result, err := processMessagesFunc.Call(store, messages)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func loadModule(store *wasmtime.Store, filename string) (*wasmtime.Module, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return wasmtime.NewModule(store.Engine, content)
}
