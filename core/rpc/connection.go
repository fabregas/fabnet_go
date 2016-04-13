package rpc

import (
	"fmt"
	"io"
	"net"
	"sync"
)

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
