package rpc

import (
	"fmt"
	"math/rand"
	"testing"
)

type HelloOpProcessor struct {
}

func (h *HelloOpProcessor) Start() error {
	return nil
}

func (h *HelloOpProcessor) ProcessRequest(req *Packet) Params {
	d := NewParams()
	d["msg"] = "hello, " + req.Data["test"]
	return d
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func test_f(conn *Connection, i int, data string, resp_chan chan *Packet) {
	d := NewParams()
	d["test"] = data
	packet := Packet{Type: PT_REQUEST, CmdID: i, Data: d}
	resp, err := conn.Send(&packet)
	if err != nil {
		fmt.Println("Send error: %s", err)
	}
	resp_chan <- resp
}

func TestRPC(t *testing.T) {
	self_node, err := NewNode("node00", "127.0.0.1:22212", new(HelloOpProcessor))
	if err != nil {
		t.Error("Cant create selfnode")
		return
	}
	go self_node.Run()

	node := NewConnection("127.0.0.1:22212")
	err = node.Connect()
	if err != nil {
		t.Error("No connection to ", node.Addr)
		return
	}

	packets_chan := make(chan *Packet)
	data_list := make([]string, 100)

	for i := 0; i < 100; i++ {
		data := string(RandStringBytes(50))
		data_list[i] = data

	}
	for i, data := range data_list {
		go test_f(node, i, data, packets_chan)
	}

	var pack *Packet
	for i := 0; i < 100; i++ {
		pack = <-packets_chan
		fmt.Printf("[%d] received back from node: %s\n", i, pack.String())
		if pack.CmdID < 0 && pack.CmdID >= 10 {
			t.Error("invalid cmdID:", pack.CmdID)
		}
		if pack.Type != PT_RESPONSE {
			t.Error("invalid packet type")
		}
		if pack.Data["msg"] != fmt.Sprintf("hello, %s", data_list[pack.CmdID]) {
			t.Error("invalid packet data: ", data_list[pack.CmdID], pack.Data["msg"])
		}
	}

	node.Close()
	err = self_node.Close()
	if err != nil {
		t.Error("SelfNode close error ", err.Error())
	}

}
