package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"../logger"
)

const (
	IN_CHAN_LEN = 5
	PT_REQUEST  = true
	PT_RESPONSE = false
)

var (
	log = logger.NewLogger("rpc")

	errInvalidWroteSize = errors.New("invalid wrote size")
	errAlreadyOpenned   = errors.New("connection already openned")
	errAlreadyClosed    = errors.New("connection already closed")
)

//------------------ Packet ------------------

type Params map[string]string

func (p Params) String() string {
	ret := "{"
	for key, val := range p {
		if len(ret) != 1 {
			ret = ret + " ,"
		}
		ret = ret + key + "=" + val + ", "
	}
	ret = ret + "}"
	return ret
}

func NewParams() Params {
	return make(Params)
}

type Packet struct {
	Type  bool
	CmdID int
	Data  Params
}

func (p *Packet) String() string {
	if p.Type == PT_REQUEST {
		return fmt.Sprintf("Request(cmd=%d, data=%s)", p.CmdID, p.Data)
	} else {
		return fmt.Sprintf("Response(cmd=%d, data=%s)", p.CmdID, p.Data)
	}
}

func packetFromRaw(buf []byte) (*Packet, error) {
	request := new(Packet)
	reader := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(reader)
	err := dec.Decode(request)

	if err != nil {
		return nil, err
	}
	return request, nil
}

func (p *Packet) serialize() ([]byte, error) {
	var resp bytes.Buffer
	enc := gob.NewEncoder(&resp)
	err := enc.Encode(p)
	if err != nil {
		log.Error("encode error: %s", err.Error())
		return nil, err
	}
	return resp.Bytes(), nil
}

//----------------- Connection--------------

type Connection struct {
	Addr string

	conn   *net.TCPConn
	closed bool
	mutex  *sync.Mutex
}

func NewConnection(addr string) *Connection {
	conn := Connection{Addr: addr, closed: true, mutex: &sync.Mutex{}}
	return &conn
}

func GetConnection(c *net.TCPConn) *Connection {
	addr := c.RemoteAddr().String()
	conn := Connection{Addr: addr, conn: c, closed: false, mutex: &sync.Mutex{}}
	return &conn
}

func (conn *Connection) String() string {
	return fmt.Sprintf("Conn(%s)", conn.Addr)
}

func (conn *Connection) Connect() (err error) {
	if conn.conn != nil {
		return errAlreadyOpenned
	}
	addr, err := net.ResolveTCPAddr("tcp", conn.Addr)
	if err != nil {
		return err
	}
	conn.conn, err = net.DialTCP("tcp", nil, addr)
	if err == nil {
		conn.closed = false
		log.Debug("established connection to %s", conn.Addr)

	}
	return err
}

func (conn *Connection) Close() error {
	if conn.closed == true {
		return errAlreadyClosed
	}
	err := conn.conn.CloseRead()
	err = conn.conn.Close()
	conn.closed = true
	log.Debug("closed connection to %s", conn.Addr)
	return err
}

func to_uint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func from_uint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func (conn *Connection) writePacket(packet *Packet) error {
	packed, err := packet.serialize()
	size_buf := make([]byte, 4)
	from_uint32(size_buf, uint32(len(packed)))
	wrote, err := conn.conn.Write(append(size_buf, packed...))
	if wrote != len(packed)+4 {
		return errInvalidWroteSize
	}
	return err
}

func (conn *Connection) readPacket() (*Packet, error) {
	var err error
	var buf []byte
	var size uint32
	size_buf := make([]byte, 4)

	_, err = io.ReadFull(conn.conn, size_buf)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		log.Error("[%s] read packet size error: %s", conn.Addr, err.Error())
		return nil, err
	}
	size = to_uint32(size_buf)
	buf = make([]byte, size)
	_, err = io.ReadFull(conn.conn, buf)
	if err != nil {
		log.Error("[%s] read socket error: %s", conn.Addr, err.Error())
		return nil, err
	}

	packet, err := packetFromRaw(buf)
	if err != nil {
		log.Error("[%s] packet from raw error: %s", conn.Addr, err.Error())
		return nil, err
	}

	return packet, nil
}

func (conn *Connection) Send(packet *Packet) (*Packet, error) {
	conn.mutex.Lock()
	err := conn.writePacket(packet)
	if err != nil {
		conn.mutex.Unlock()
		return nil, err
	}
	resp, err := conn.readPacket()
	conn.mutex.Unlock()

	if err != nil {
		return nil, err
	}
	return resp, nil
}

//------------------ Node ------------------

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
