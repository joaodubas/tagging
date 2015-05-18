package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	conn, err := NewClient("127.0.0.1:6379")
	handleError("Error connecting with redis:", err)
	defer conn.Close()

	conn.Set("key:string", "ola")
	r, err := conn.Get("key:string")
	handleError("Error getting key:", err)
	fmt.Printf("the key %s is %s\n", "key:string", r)

	conn.Sadd("key:set", "ola", "mundo")
	r, err = conn.Scard("key:set")
	handleError("Error getting key:", err)
	fmt.Printf("the key %s has len %s\n", "key:set", r)
	r, err = conn.Smembers("key:set")
	handleError("Error getting key:", err)
	for _, e := range r.Elems {
		fmt.Printf("the key %s has elem %s\n", "key:set", e)
	}

	nconn, err := NewClient("127.0.0.1:6379")
	handleError("Error getting key:", err)
	psconn := NewPSClient(nconn)
	defer psconn.Close()

	sub := psconn.Subscribe("channel:test")
	r, err = conn.Publish("channel:test", "hello")
	handleError("Did not publish message:", err)
	sub = psconn.Receive()
	if !sub.Timeout() {
		fmt.Printf("Received message from channel %s %s\n", sub.Channel, sub.Message)
	} else {
		handleError("Timeout for receive message", fmt.Errorf("Timeout"))
	}

	conn.Multi()
	conn.Add("set", "key:pipe:1", "ola")
	conn.Add("set", "key:pipe:2", "hello")
	conn.Add("set", "key:pipe:3", "oie")
	conn.Add("set", "key:pipe:4", "hi")
	ar1 := conn.Add("get", "key:pipe:1")
	ar2 := conn.Add("get", "key:pipe:2")
	ar3 := conn.Add("get", "key:pipe:3")
	ar4 := conn.Add("get", "key:pipe:4")
	r, err = conn.Exec()
	handleError("Something went wrong with pipe:", err)
	fmt.Printf("Responses: %s, %s, %s, %s\n", <-ar1, <-ar2, <-ar3, <-ar4)
	for i, e := range r.Elems {
		fmt.Printf("Result for op %d: %s\n", i, e)
	}

	conn.Multi()
	conn.Add("set", "key:pipe:1", "ola")
	conn.Add("set", "key:pipe:2", "hello")
	conn.Add("get", "key:pipe:1")
	conn.Add("get", "key:pipe:2")
	r, err = conn.Discard()
	handleError("Something went wrong with pipe:", err)
	fmt.Printf("Result for discard %s", r)
}

func handleError(msg string, err error) {
	if err != nil {
		log.Fatal(msg, err)
		os.Exit(1)
	}
}
