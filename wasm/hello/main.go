package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v18"
)

var helloModule *wasmtime.Module
var store *wasmtime.Store

func init() {
	store = wasmtime.NewStore(wasmtime.NewEngine())
	var err error

	helloModule, err = loadModule(store, "./hello.wasm")
	if err != nil {
		panic(err)
	}
	slog.Info("loaded module")
}

func loadModule(store *wasmtime.Store, filename string) (*wasmtime.Module, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return wasmtime.NewModule(store.Engine, content)
}

func main() {
	helloInstance, err := wasmtime.NewInstance(store, helloModule, []wasmtime.AsExtern{})
	if err != nil {
		panic(err)
	}

	registerActorFunc := helloInstance.GetFunc(store, "add")
	if registerActorFunc == nil {
		panic(fmt.Errorf("failed to find function `add` in the store module"))
	}

	now := time.Now()
	for i := 0; i < 1000000; i++ {
		_, err := registerActorFunc.Call(store, 6, 3)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println(time.Since(now))
}
