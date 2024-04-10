package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership wraps Serf to provide discovery and cluster membership to our service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

type Config struct {
	// NodeName acts as the node's unique identifier
	NodeName       string
	BindAddrr      string
	Tags           map[string]string
	StartJoinAddrs []string
}

func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddrr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	// StartJoinAddrs -- when you have an existing cluster and you create a new node that you want to add to that cluster
	// you need to point your new node to at least one of the nodes in the cluster.
	// In production, specify at least three addresses to make your cluster resilient to one or two node failures or a disrupted network.
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// eventHandler runs in a loop reading events sent by Serf into the events channel.
// Handling each incoming event according to the event's type.
// When a node joins or leaves the cluster, Serf sends an event to all nodes, including the node
// that joined or left the cluster.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					// skip itself
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.LogError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.LogError(err, "failed to leave", member)
	}
}

// isLocal returns whether the given Serf member is the local member by checking the member's name.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns a point-in-time snapshot of the cluster's Serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells this member to leave the Serf cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// LogError logs the given error and message.
func (m *Membership) LogError(err error, msg string, member serf.Member) {
	m.logger.Error(msg, zap.Error(err), zap.String("name", member.Name), zap.String("rpc_addr", member.Tags["rpc_addr"]))
}

// Handler represents some component in our service that needs to know when a server joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}
