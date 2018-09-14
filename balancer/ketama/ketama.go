// Package ketama defines a ketama balancer. Roundrobin balancer is
// installed as one of the default balancers in gRPC, users don't need to
// explicitly install this balancer.
package ketama

import (
	"hash/crc32"
	"sort"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

// Name is the name of ketama balancer.
const Name = "ketama"
const DefaultKetamaKey = "_grpclb-ketama-key"
const defaultReplicas = 10

// newBuilder creates a new ketama balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &kPickerBuilder{})
}

func init() {
	balancer.Register(newBuilder())
}

type kPickerBuilder struct{}

func (*kPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("ketamaPicker: newPicker called with readySCs: %v", readySCs)
	scs := map[int]balancer.SubConn{}
	hashs := []int{}
	for addr, sc := range readySCs {
		for i := 0; i < defaultReplicas; i++ {
			h := int(crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + addr.Addr)))

			hashs = append(hashs, h)
			scs[h] = sc
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

func (p *kPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.subConns) <= 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}

	pos := len(p.subConns) - 1
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, data := range md[DefaultKetamaKey] {
			hash := int(crc32.ChecksumIEEE([]byte(data)))
			pos = sort.Search(len(p.connHashs), func(i int) bool {
				return p.connHashs[i] >= hash
			})

			break
		}
	}

	sc := p.subConns[pos]
	return sc, nil, nil
}
