package main

import (
	"embed"
	"net/http"
)

//go:embed main.js index.html
var staticFiles embed.FS

func main() {
	// Serve files embedded from the root.
	http.Handle("/", http.FileServer(http.FS(staticFiles)))
	println("Listening on http://localhost:8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}
