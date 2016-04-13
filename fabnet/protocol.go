package fabnet

import (
	"errors"
	"fmt"
	"github.com/fabregas/fabnet_go/logger"
	"github.com/fabregas/fabnet_go/rpc"
	"strconv"
	"sync"
)

const (
	// protocol commands
	CMD_JOIN         = 1
	CMD_FORWARD_JOIN = 2
	CMD_DISCONNECT   = 3
	CMD_NEIGHBOR     = 4

	// forward_join modes
	ONE = false
	ALL = true
)

var (
	log = logger.NewLogger("protocol")
)

type FabnetConfig struct {
	Addr           string // node address - ip:port
	Name           string // node identifier
	BootstrapNodes []string

	ActiveMembership  int
	PassiveMembership int
	ARWL              int // Active Random Walk Length
	PRWL              int // Passive Random Walk Length
}

func DefaultConfig(name, addr string, bootstrapNodes []string) *FabnetConfig {
	return &FabnetConfig{Addr: addr, Name: name, BootstrapNodes: bootstrapNodes, ActiveMembership: 5, PassiveMembership: 30, ARWL: 6, PRWL: 3}
}

type FabnetProcessor struct {
	config      *FabnetConfig
	activeView  map[string]*rpc.Connection
	passiveView map[string]*rpc.Connection
	mutex       *sync.Mutex
}

func NewFabnetProcessor(config *FabnetConfig) *FabnetProcessor {
	return &FabnetProcessor{config, make(map[string]*rpc.Connection), make(map[string]*rpc.Connection), &sync.Mutex{}}
}

func (proc *FabnetProcessor) String() string {
	proc.mutex.Lock()
	ret := fmt.Sprintf("NODE(name=%s, address=%s)\n", proc.config.Name, proc.config.Addr)
	ret += fmt.Sprintf("\tACTIVE VIEW:\n")
	for id, node := range proc.activeView {
		ret += fmt.Sprintf("\t\t%s - %s\n", id, node)
	}
	ret += fmt.Sprintf("\tPASSIVE VIEW:\n")
	for id, node := range proc.passiveView {
		ret += fmt.Sprintf("\t\t%s - %s\n", id, node)
	}
	proc.mutex.Unlock()
	return ret
}

func (proc *FabnetProcessor) dumpActiveView() rpc.Params {
	ret := rpc.NewParams()
	proc.mutex.Lock()
	for id, node := range proc.activeView {
		ret[id] = node.Addr
	}
	proc.mutex.Unlock()
	return ret
}

func (proc *FabnetProcessor) dumpPassiveView() rpc.Params {
	ret := rpc.NewParams()
	proc.mutex.Lock()
	for id, node := range proc.passiveView {
		ret[id] = node.Addr
	}
	proc.mutex.Unlock()
	return ret
}

func (proc *FabnetProcessor) Bootstrap() error {
	var err error
	data := rpc.NewParams()
	data["id"] = proc.config.Name
	data["addr"] = proc.config.Addr
	data["sender"] = proc.config.Name
	data["force"] = "true"
	for _, addr := range proc.config.BootstrapNodes {
		a_node := rpc.NewConnection(addr)
		err = a_node.Connect()
		if err != nil {
			log.Warning("[%s] Cant bootstrap over %s: %s", proc.config.Name, addr, err.Error())
			continue
		}
		packet := rpc.Packet{Type: rpc.PT_REQUEST, CmdID: CMD_JOIN, Data: data}
		_, err := a_node.Send(&packet)
		if err != nil {
			log.Warning("[%s] Cant bootstrap over %s: %s", proc.config.Name, addr, err.Error())
			continue
		}
	}
	return err
}

func (proc *FabnetProcessor) addNodeActiveView(data rpc.Params, ask_neighbor bool) error {
	id, ok := data["id"]
	if !ok {
		return errors.New("'id' of node expected")
	}

	address, ok := data["addr"]
	if !ok {
		return errors.New("'addr' of node expected")
	}

	_, ok = proc.activeView[id]
	if ok {
		//already in active view
		//TODO check metadata and update if need
		return nil
	}
	_, ok = proc.passiveView[id]
	if ok {
		//contain in passiveView, just remove
		delete(proc.passiveView, id)
	}
	if len(proc.activeView) == proc.config.ActiveMembership {
		// Active view is full!
		force, ok := data["force"]
		if ok && force == "true" {
			proc.dropRandomActiveViewNode()
		} else {
			proc.addNodePassiveView(data)
			return nil
		}
	}

	a_node := rpc.NewConnection(address)
	err := a_node.Connect()
	if err != nil {
		return err
	}
	if ask_neighbor {
		req_data := rpc.NewParams()
		req_data["id"] = proc.config.Name
		req_data["addr"] = proc.config.Addr
		req_data["force"] = "1"

		callback := func(resp *rpc.Packet) {
			proc.mutex.Lock()
			if resp != nil && resp.Data["flag"] == "true" {
				proc.activeView[id] = a_node
			} else {
				log.Debug("[%s] Cant add node %s to activeView, bcs it reject friend's request", proc.config.Name, id)
				proc.addNodePassiveView(data)
			}
			proc.mutex.Unlock()
		}
		go proc.sendToNode(a_node, CMD_NEIGHBOR, req_data, callback)
	} else {
		proc.activeView[id] = a_node
	}
	return nil
}

func getSomeMapKey(m map[string]*rpc.Connection) string {
	for key := range m {
		return key
	}
	return ""
}

func (proc *FabnetProcessor) dropRandomActiveViewNode() {
	rkey := getSomeMapKey(proc.activeView)
	node := proc.activeView[rkey]
	req_data := rpc.NewParams()
	req_data["id"] = proc.config.Name

	callback := func(_ *rpc.Packet) {
		node.Close()
	}
	go proc.sendToNode(node, CMD_DISCONNECT, req_data, callback)
	delete(proc.activeView, rkey)

	proc.moveToPassive(rkey, node)
}

func (proc *FabnetProcessor) addNodePassiveView(data rpc.Params) {
	id := data["id"]
	address := data["addr"]

	_, ok := proc.activeView[id]
	if ok {
		log.Debug("[%s] addNodePassiveView: node %s is in active view, skipping... ", proc.config.Name, id)
		return
	}

	p_node := rpc.NewConnection(address)
	proc.moveToPassive(id, p_node)
}

func (proc *FabnetProcessor) removeFromActiveView(data rpc.Params) {
	id, ok := data["id"]
	if !ok {
		log.Warning("'id' of node expected")
		return
	}

	node, ok := proc.activeView[id]
	if !ok {
		// node not in active view
		return
	}
	delete(proc.activeView, id)

	proc.moveToPassive(id, node)
}

func (proc *FabnetProcessor) moveToPassive(id string, node *rpc.Connection) {
	if len(proc.passiveView) == proc.config.PassiveMembership {
		// Passive view is full, drop random node from passive view
		rkey := getSomeMapKey(proc.passiveView)
		delete(proc.passiveView, rkey)
	}
	proc.passiveView[id] = node
}

func (proc *FabnetProcessor) forwardJoin(data rpc.Params, ttl int, send_mode bool) {
	id := data["id"]
	sender := data["sender"]
	req_data := rpc.NewParams()
	req_data["id"] = id
	req_data["TTL"] = strconv.Itoa(ttl)
	req_data["addr"] = data["addr"]
	req_data["sender"] = proc.config.Name
	for anode_id, act_node := range proc.activeView {
		if anode_id == sender {
			continue
		}
		go proc.sendToNode(act_node, CMD_FORWARD_JOIN, req_data, nil)
		if send_mode == ONE {
			break
		}
	}
}

func (proc *FabnetProcessor) processNeighbor(data rpc.Params) string {
	_, ok := data["force"]
	if ok || len(proc.activeView) < proc.config.ActiveMembership {
		proc.addNodeActiveView(data, false)
		return "true"
	}
	return "false"
}

func (proc *FabnetProcessor) sendToNode(act_node *rpc.Connection, cmd_id int, data rpc.Params, cb func(*rpc.Packet)) {
	packet := rpc.Packet{Type: rpc.PT_REQUEST, CmdID: cmd_id, Data: data}
	resp, err := act_node.Send(&packet)
	if err != nil {
		log.Warning("[%s] Can't send packet %s to %s", proc.config.Name, packet, act_node)
	}
	if cb != nil {
		cb(resp)
	}

}

func (proc *FabnetProcessor) ProcessRequest(req *rpc.Packet) rpc.Params {
	resp := rpc.NewParams()
	proc.mutex.Lock()

	switch req.CmdID {
	case CMD_JOIN:
		log.Debug("[%s] ===> JOIN %s", proc.config.Name, req.Data)
		err := proc.addNodeActiveView(req.Data, true)
		if err != nil {
			resp["error"] = err.Error()
			break
		}
		proc.forwardJoin(req.Data, proc.config.ARWL, ALL)
		resp["id"] = proc.config.Name

	case CMD_FORWARD_JOIN:
		log.Debug("[%s] ===> FORWARD_JOIN %s", proc.config.Name, req.Data)
		ttl, err := strconv.Atoi(req.Data["TTL"])
		if err != nil {
			resp["error"] = err.Error()
			break
		}
		if ttl == 0 || len(proc.activeView) == 1 {
			proc.addNodeActiveView(req.Data, true)
		} else {
			if ttl == proc.config.PRWL {
				log.Debug("[%s] TTL=PRWL, adding node to passive view", proc.config.Name)
				proc.addNodePassiveView(req.Data)
			}
			proc.forwardJoin(req.Data, ttl-1, ONE)
		}

	case CMD_DISCONNECT:
		log.Debug("[%s] ===> DISCONNECT %s", proc.config.Name, req.Data)
		proc.removeFromActiveView(req.Data)

	case CMD_NEIGHBOR:
		log.Debug("[%s] ===> NEIGHBOR %s", proc.config.Name, req.Data)
		add_flag := proc.processNeighbor(req.Data)
		resp["flag"] = add_flag
	}

	proc.mutex.Unlock()
	return resp
}
