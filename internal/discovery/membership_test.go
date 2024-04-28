package discovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	. "github.com/albscui/proglog/internal/discovery"
)

// Our test sets up a cluster with multiple servers and checks that the Membership returns all the servers that joined.
func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && len(m[0].Members()) == 3 && len(handler.leaves) == 0
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

// setupMember sets up a new member under a a free port and with the member's length as the node name so the names are unique.
// The member's length also tells us whether this member is the cluster's initial member or we have a cluster to join.
func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	t.Helper()

	id := fmt.Sprintf("%d", len(members))
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{"rpc_addr": addr}
	cfg := Config{
		NodeName:  id,
		BindAddrr: addr,
		Tags:      tags,
	}
	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		cfg.StartJoinAddrs = []string{members[0].BindAddrr}
	}
	m, err := New(h, cfg)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

// mock handler tracks how many times our Membership calls the handler's Join and Leave methods.
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
