package main

import (
	"log"
	"net/http"
)

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello world"))
}

func main() {
	http.HandleFunc("GET /", helloGoHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
