package main
import (
	"net/rpc"
	"fmt"
)

type Args struct {
	A, B int
}

func main() {
	args := Args{A: 2, B: 3}
	var reply *int
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}

	client.Call("Arith.Add", args, &reply)
	
	fmt.Println("Result:", *reply)
}