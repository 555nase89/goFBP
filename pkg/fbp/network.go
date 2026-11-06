package fbp

import (
	"errors"
	"fmt"
	"slices"
)

// Network is a graph of Nodes linked by Connections, with at least the
// Inport In and the Outports Success and Error. Like a Brick it is a
// definition only; Start creates a running Instance from it.
type Network struct {
	Name        string
	Description string
	// Inports lists additional Inports; In always exists.
	Inports []string
	// Outports lists additional Outports; Success and Error always exist.
	Outports []string

	nodes    map[string]Node
	order    []string
	conns    []connection
	inConns  []inConnection
	outConns []outConnection
}

type connection struct {
	from, outport, to, inport string
}

type inConnection struct {
	netInport, to, inport string
}

type outConnection struct {
	from, outport, netOutport string
}

func (n *Network) inportNames() []string {
	return withNames([]string{In}, n.Inports)
}

func (n *Network) outportNames() []string {
	return withDefaults(n.Outports)
}

// Add adds node, a *Brick or a nested *Network, under nodeName. The same
// Brick or Network may be added several times under different node names, and
// different Bricks may share a name; Nodes are told apart by their node name.
// A Network must not contain itself, neither directly nor through a nested
// Network.
func (n *Network) Add(nodeName string, node Node) error {
	if nodeName == "" {
		return errors.New("fbp: empty node name")
	}
	if _, ok := n.nodes[nodeName]; ok {
		return fmt.Errorf("fbp: node %q already exists", nodeName)
	}
	switch nd := node.(type) {
	case *Brick:
		if nd == nil {
			return fmt.Errorf("fbp: node %q is nil", nodeName)
		}
		if err := nd.validate(); err != nil {
			return err
		}
	case *Network:
		if nd == nil {
			return fmt.Errorf("fbp: node %q is nil", nodeName)
		}
		if nd == n || nd.contains(n) {
			return fmt.Errorf("fbp: network %q must not contain itself", n.Name)
		}
	default:
		return fmt.Errorf("fbp: node %q has unsupported type %T", nodeName, node)
	}
	if n.nodes == nil {
		n.nodes = map[string]Node{}
	}
	n.nodes[nodeName] = node
	n.order = append(n.order, nodeName)
	return nil
}

// Connect connects outport of node from to the Inport In of node to.
func (n *Network) Connect(from, outport, to string) error {
	return n.ConnectTo(from, outport, to, In)
}

// ConnectTo connects outport of node from to inport of node to. Only a nested
// Network can have an Inport other than In.
func (n *Network) ConnectTo(from, outport, to, inport string) error {
	if err := n.checkOutport(from, outport); err != nil {
		return err
	}
	if err := n.checkInport(to, inport); err != nil {
		return err
	}
	c := connection{from, outport, to, inport}
	if slices.Contains(n.conns, c) {
		return fmt.Errorf("fbp: %s.%s is already connected to %s.%s", from, outport, to, inport)
	}
	n.conns = append(n.conns, c)
	return nil
}

// ConnectIn connects the Network's Inport In to the Inport In of node to.
func (n *Network) ConnectIn(to string) error {
	return n.ConnectInportTo(In, to, In)
}

// ConnectInport connects the Network's Inport netInport to the Inport In of
// node to.
func (n *Network) ConnectInport(netInport, to string) error {
	return n.ConnectInportTo(netInport, to, In)
}

// ConnectInportTo connects the Network's Inport netInport to inport of node
// to. Only a nested Network can have an Inport other than In.
func (n *Network) ConnectInportTo(netInport, to, inport string) error {
	if !slices.Contains(n.inportNames(), netInport) {
		return fmt.Errorf("fbp: network %q has no inport %q", n.Name, netInport)
	}
	if err := n.checkInport(to, inport); err != nil {
		return err
	}
	c := inConnection{netInport, to, inport}
	if slices.Contains(n.inConns, c) {
		return fmt.Errorf("fbp: network inport %s is already connected to %s.%s", netInport, to, inport)
	}
	n.inConns = append(n.inConns, c)
	return nil
}

// ConnectOut connects outport of node from to the Network's Outport netOutport.
func (n *Network) ConnectOut(from, outport, netOutport string) error {
	if err := n.checkOutport(from, outport); err != nil {
		return err
	}
	if !slices.Contains(withDefaults(n.Outports), netOutport) {
		return fmt.Errorf("fbp: network %q has no outport %q", n.Name, netOutport)
	}
	c := outConnection{from, outport, netOutport}
	if slices.Contains(n.outConns, c) {
		return fmt.Errorf("fbp: %s.%s is already connected to network outport %s", from, outport, netOutport)
	}
	n.outConns = append(n.outConns, c)
	return nil
}

func (n *Network) checkInport(node, inport string) error {
	nd, ok := n.nodes[node]
	if !ok {
		return fmt.Errorf("fbp: unknown node %q", node)
	}
	if !slices.Contains(nd.inportNames(), inport) {
		return fmt.Errorf("fbp: node %q has no inport %q", node, inport)
	}
	return nil
}

func (n *Network) checkOutport(node, outport string) error {
	nd, ok := n.nodes[node]
	if !ok {
		return fmt.Errorf("fbp: unknown node %q", node)
	}
	if !slices.Contains(nd.outportNames(), outport) {
		return fmt.Errorf("fbp: node %q has no outport %q", node, outport)
	}
	return nil
}

// contains reports whether target is nested in n at any depth.
func (n *Network) contains(target *Network) bool {
	for _, node := range n.nodes {
		if sub, ok := node.(*Network); ok && (sub == target || sub.contains(target)) {
			return true
		}
	}
	return false
}

// Validate checks the Network and all nested Networks: each needs a name,
// named ports, every Inport connected and its Connections must not form a
// cycle. Since every nested Network is acyclic as well, there is no cycle
// across nesting levels.
func (n *Network) Validate() error {
	if n.Name == "" {
		return errors.New("fbp: network has no name")
	}
	if slices.Contains(n.Outports, "") {
		return fmt.Errorf("fbp: network %q has an outport without name", n.Name)
	}
	if slices.Contains(n.Inports, "") {
		return fmt.Errorf("fbp: network %q has an inport without name", n.Name)
	}
	for _, in := range n.inportNames() {
		if !slices.ContainsFunc(n.inConns, func(c inConnection) bool { return c.netInport == in }) {
			return fmt.Errorf("fbp: inport %q of network %q is not connected", in, n.Name)
		}
	}
	if err := n.checkCycles(); err != nil {
		return err
	}
	for _, name := range n.order {
		if sub, ok := n.nodes[name].(*Network); ok {
			if err := sub.Validate(); err != nil {
				return fmt.Errorf("node %q of network %q: %w", name, n.Name, err)
			}
		}
	}
	return nil
}

// checkCycles runs a depth-first search over the Connections.
func (n *Network) checkCycles() error {
	next := map[string][]string{}
	for _, c := range n.conns {
		next[c.from] = append(next[c.from], c.to)
	}
	const (
		unvisited = iota
		visiting
		done
	)
	state := map[string]int{}
	var visit func(node string, path []string) error
	visit = func(node string, path []string) error {
		path = append(path, node)
		switch state[node] {
		case visiting:
			return fmt.Errorf("fbp: network %q contains a cycle: %v", n.Name, path)
		case done:
			return nil
		}
		state[node] = visiting
		for _, to := range next[node] {
			if err := visit(to, path); err != nil {
				return err
			}
		}
		state[node] = done
		return nil
	}
	for _, node := range n.order {
		if err := visit(node, nil); err != nil {
			return err
		}
	}
	return nil
}
