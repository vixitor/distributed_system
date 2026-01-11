package main

import (
	"net"
	"net/rpc"
	"fmt"
)

type Args struct { 
	A, B int
}
type Arith struct {}
func (a *Arith) Add (args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}
func main() {
	rpc.Register(new(Arith))
	
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server is listening on", listener.Addr())
	rpc.Accept(listener)
}

