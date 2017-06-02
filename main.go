package main

import (
    "fmt"
    "net/http"
    "flag"
)

func handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func main() {
    var port int;
    flag.IntVar(&port, "port", 8080, "The port to listen on");
    flag.Parse();
    http.HandleFunc("/", handler)
    http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
