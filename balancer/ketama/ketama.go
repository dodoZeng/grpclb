// Package ketama defines a ketama balancer. Roundrobin balancer is
// installed as one of the default balancers in gRPC, users don't need to
// explicitly install this balancer.
package ketama

import (
	"sort"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	consul_api "github.com/hashicorp/consul/api"
)

// BalancerName is the name of ketama balancer.
const BalancerName = "ketama"

// Key is the name of Key in the request
const Key = "_grpclb-ketama-key"

// newBuilder creates a new ketama balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(BalancerName, &kPickerBuilder{})
}

func init() {
	balancer.Register(newBuilder())
}

type kPickerBuilder struct{}

func (*kPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	//grpclog.Infof("ketamaPicker: newPicker called with readySCs: %v", readySCs)
	scs := map[int]balancer.SubConn{}
	hashs := []int{}
	for addr, sc := range readySCs {
		if s, ok := addr.Metadata.(*consul_api.AgentService); ok {
			if h, err := strconv.Atoi(s.Meta["hash"]); err == nil {
				hashs = append(hashs, h)
				scs[h] = sc
			}
		}
	}
	sort.Ints(hashs)

	return &kPicker{
		subConns:  scs,
		connHashs: hashs,
	}
}

type kPicker struct {
	// subConns is the snapshot of the ketama balancer when this picker was
	// created. The slice is immutable. Each Get() will do a hashing
	// selection from it and return the selected SubConn.
	subConns  map[int]balancer.SubConn
	connHashs []int
}

func (p *kPicker) Pick(ctx context.Context, opts balancer.PickInfo) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.subConns) <= 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}

	pos := len(p.connHashs) - 1
	if key, ok := ctx.Value(Key).(string); ok {
		//hash := int(crc32.ChecksumIEEE([]byte(key)))
		hash, _ := strconv.Atoi(key)

		pos = sort.Search(len(p.connHashs), func(i int) bool {
			return hash <= p.connHashs[i]
		})
		if pos >= len(p.connHashs) {
			pos = len(p.connHashs) - 1
		}
	}

	h := p.connHashs[pos]
	if sc, ok := p.subConns[h]; ok {
		return sc, nil, nil
	}

	return nil, nil, balancer.ErrNoSubConnAvailable
}
