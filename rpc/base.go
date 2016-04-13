package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/fabregas/fabnet_go/logger"
)

const (
	PT_REQUEST  = true
	PT_RESPONSE = false
)

var (
	log = logger.NewLogger("rpc")

	errInvalidWroteSize = errors.New("invalid wrote size")
	errAlreadyOpenned   = errors.New("connection already openned")
	errAlreadyClosed    = errors.New("connection already closed")
)

type Params map[string]string

func (p Params) String() string {
	ret := "{"
	for key, val := range p {
		if len(ret) != 1 {
			ret = ret + ", "
		}
		ret = ret + key + "=" + val
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
