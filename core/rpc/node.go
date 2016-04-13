package rpc

import (
	"net"
)

type ApiProcessor interface {
	ProcessRequest(req *Packet) (resp Params)
}

type Node struct {
	Name      string
	Addr      string
	Processor ApiProcessor

	listener *net.TCPListener
	finished chan bool
}

func NewNode(name, addr string, processor ApiProcessor) (*Node, error) {
	netAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenTCP("tcp", netAddr)
	if err != nil {
		return nil, err
	}
	log.Debug("Node binded at %s", addr)

	return &Node{
		Name:      name,
		Addr:      addr,
		Processor: processor,
		listener:  ln,
		finished:  make(chan bool),
	}, nil
}

func (node *Node) Run() (err error) {
	for {
		conn, err := node.listener.AcceptTCP()
		if err != nil {
			log.Debug("Accept error: %s", err.Error())
			break
		}
		log.Debug("Accept connection from: %s", conn.RemoteAddr().String())
		go node.handleConnection(conn)
	}
	node.finished <- true
	return
}

func (node *Node) Close() error {
	err := node.listener.Close()
	if err != nil {
		return err
	}
	<-node.finished
	return nil
}

func (node *Node) handleConnection(conn *net.TCPConn) {
	rConn := GetConnection(conn)
	for {
		packet, err := rConn.readPacket()
		if err != nil {
			break
		}
		data := node.Processor.ProcessRequest(packet)
		resp := Packet{Type: PT_RESPONSE, CmdID: packet.CmdID, Data: data}
		err = rConn.writePacket(&resp)
		if err != nil {
			log.Error("Write to socket error: %s", err.Error())
			break
		}
	}
	rConn.Close()
}
