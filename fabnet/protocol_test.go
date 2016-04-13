package fabnet

import (
	"fmt"
	"github.com/fabregas/fabnet_go/rpc"
	"testing"
	"time"
)

/*
func TestRPC(t *testing.T) {
	conf0 := DefaultConfig("node00", "127.0.0.1:22212", []string{})
	proc0 := NewFabnetProcessor(conf0)
	node00, err := rpc.NewNode(conf0.Name, conf0.Addr, proc0)
	if err != nil {
		t.Error("Cant create node00")
		return
	}
	go node00.Run()

	conf1 := DefaultConfig("node01", "127.0.0.1:22213", []string{"127.0.0.1:22212"})
	proc1 := NewFabnetProcessor(conf1)
	node01, err := rpc.NewNode(conf1.Name, conf1.Addr, proc1)
	if err != nil {
		t.Error("Cant create node01")
		return
	}
	go node01.Run()

	err = proc1.Bootstrap()
	if err != nil {
		t.Error("No bootstrap ", err.Error())
		return
	}

	time.Sleep(time.Millisecond * 1000)

	conf2 := DefaultConfig("node02", "127.0.0.1:22214", []string{"127.0.0.1:22213"})
	proc2 := NewFabnetProcessor(conf2)
	node02, err := rpc.NewNode(conf2.Name, conf2.Addr, proc2)
	if err != nil {
		t.Error("Cant create node01")
		return
	}
	go node02.Run()

	err = proc2.Bootstrap()
	if err != nil {
		t.Error("No bootstrap ", err.Error())
		return
	}
	time.Sleep(time.Millisecond * 2000)

	fmt.Println(node00.Processor)
	fmt.Println(node01.Processor)
	fmt.Println(node02.Processor)
}

func TestRPCMany(t *testing.T) {
	bootstrap_nodes := []string{}
	init_port := 9900
	var s_node *rpc.Node
	for i := 0; i < 50; i++ {
		conf := DefaultConfig(fmt.Sprintf("node%d", i), fmt.Sprintf("127.0.0.1:%d", init_port+i), bootstrap_nodes)
		proc := NewFabnetProcessor(conf)
		node, err := rpc.NewNode(conf.Name, conf.Addr, proc)
		if err != nil {
			t.Error(fmt.Sprintf("Cant create node: %s", err.Error()))
			return
		}
		go node.Run()
		if len(bootstrap_nodes) == 0 {
			bootstrap_nodes = []string{"127.0.0.1:9900"}
			s_node = node
		} else {
			go proc.Bootstrap()
		}

	}

	time.Sleep(time.Millisecond * 4000)
	fmt.Println(s_node.Processor)
}
*/

func waitNodeInActView(node *rpc.Node, id string) int {
	proc := node.Processor.(*FabnetProcessor)
	for i := 0; i < 1000; i++ {
		act_view := proc.dumpActiveView()
		_, ok := act_view[id]
		if ok {
			return len(act_view)
		}
		time.Sleep(time.Millisecond * 10)
	}
	panic("waitNodeInActView error")
}

func checkInterconnected(nodes map[string]*rpc.Node) {
	for id, node := range nodes {
		proc := node.Processor.(*FabnetProcessor)
		act_view := proc.dumpActiveView()
		for c_id, _ := range act_view {
			c_node, ok := nodes[c_id]
			if !ok {
				panic("NODE NOT FOUND")
			}
			c_proc := c_node.Processor.(*FabnetProcessor)
			c_act_view := c_proc.dumpActiveView()
			addr, ok := c_act_view[id]
			if !ok {
				panic("NODES NOT INTERCONNECTED")
			}
			if addr != node.Addr {
				panic("INVALID NODE ADDR")
			}
			fmt.Println(id, " <<==>> ", c_id)
		}

	}
}

func newNode(id string, port string, bootstrap_port string) *rpc.Node {
	bn := []string{}
	if bootstrap_port != "" {
		bn = append(bn, "127.0.0.1:"+bootstrap_port)
	}

	conf := DefaultConfig(id, "127.0.0.1:"+port, bn)
	proc := NewFabnetProcessor(conf)
	node, err := rpc.NewNode(conf.Name, conf.Addr, proc)
	if err != nil {
		panic("Cant create Node")
	}
	go node.Run()
	if bootstrap_port != "" {
		err = proc.Bootstrap()
		if err != nil {
			panic("No bootstrap")
		}
	}
	return node
}

func TestRPCFlow(t *testing.T) {
	all_nodes := make(map[string]*rpc.Node)
	node00 := newNode("node00", "23220", "")
	node01 := newNode("node01", "23221", "23220")
	all_nodes["node00"] = node00
	all_nodes["node01"] = node01

	time.Sleep(time.Millisecond * 100)
	checkInterconnected(all_nodes)

	node02 := newNode("node02", "23222", "23221")
	time.Sleep(time.Millisecond * 100)
	all_nodes["node02"] = node02
	checkInterconnected(all_nodes)

	node03 := newNode("node03", "23223", "23220")
	time.Sleep(time.Millisecond * 100)
	all_nodes["node03"] = node03
	checkInterconnected(all_nodes)

	node04 := newNode("node04", "23224", "23223")
	time.Sleep(time.Millisecond * 100)
	all_nodes["node04"] = node04
	checkInterconnected(all_nodes)

	fmt.Println(node00.Processor)
	fmt.Println(node01.Processor)
	fmt.Println(node02.Processor)
	fmt.Println(node03.Processor)
	fmt.Println(node04.Processor)
}
