// Package random defines a random balancer. Roundrobin balancer is
// installed as one of the default balancers in gRPC, users don't need to
// explicitly install this balancer.
package random

import (
	"math/rand"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

// Name is the name of random balancer.
const Name = "random"

// newBuilder creates a new random balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &rPickerBuilder{})
}

func init() {
	balancer.Register(newBuilder())
}

type rPickerBuilder struct{}

func (*rPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("randomPicker: newPicker called with readySCs: %v", readySCs)
	var scs []balancer.SubConn
	for _, sc := range readySCs {
		scs = append(scs, sc)
	}
	return &rPicker{
		subConns: scs,
	}
}

type rPicker struct {
	// subConns is the snapshot of the random balancer when this picker was
	// created. The slice is immutable. Each Get() will do a random
	// selection from it and return the selected SubConn.
	subConns []balancer.SubConn
}

func (p *rPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.subConns) <= 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}

	sc := p.subConns[rand.Intn(len(p.subConns))]
	return sc, nil, nil
}
