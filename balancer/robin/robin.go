// Package robin defines a robin balancer. robin balancer is
// installed as one of the default balancers in gRPC, users don't need to
// explicitly install this balancer.
package robin

import (
	"fmt"
	"math/rand"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"

	consul_api "github.com/hashicorp/consul/api"
)

// BalancerName is the name of robin balancer.
const BalancerName = "robin"

// newBuilder creates a new robin balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(BalancerName, &rPickerBuilder{})
}

func init() {
	balancer.Register(newBuilder())
}

type rPickerBuilder struct{}

func (*rPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("robinPicker: newPicker called with readySCs: %v", readySCs)

	picker := rPicker{
		step:      0,
		sumWeight: 0,
		curPos:    0,
		curIndex:  0,
	}
	total := 0

	for addr, sc := range readySCs {
		w := 1
		if s, ok := addr.Metadata.(*consul_api.AgentService); ok {
			if n, err := strconv.Atoi(s.Meta["weight"]); err == nil {
				w = n
			}
		}

		total += w
		picker.subConns = append(picker.subConns, sc)
		picker.upperWeights = append(picker.upperWeights, total)
	}

	if n := len(picker.upperWeights); n > 0 {
		picker.step = total / n
		picker.sumWeight = total
	}

	return &picker
}

type rPicker struct {
	// subConns is the snapshot of the robin balancer when this picker was
	// created. The slice is immutable. Each Get() will do a robin
	// selection from it and return the selected SubConn.
	subConns     []balancer.SubConn
	upperWeights []int
	sumWeight    int
	step         int
	curPos       int
	curIndex     int
}

func (p *rPicker) Pick(ctx context.Context, opts balancer.PickInfo) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.subConns) <= 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}

	// move one step
	p.curPos = 0
	p.curIndex = 0
	for leftSpan := rand.Intn(p.sumWeight) + 1; leftSpan > 0; {
		curSpan := p.upperWeights[p.curIndex] - p.curPos

		if leftSpan <= curSpan {
			break
		}

		leftSpan -= curSpan
		p.curPos = p.upperWeights[p.curIndex]
		if p.curPos == p.sumWeight {
			break
		} else {
			p.curIndex = (p.curIndex + 1) % len(p.upperWeights)
		}
	}
	sc := p.subConns[p.curIndex]

	// for leftSpan := p.step; leftSpan > 0; {
	// 	if p.curPos == p.sumWeight {
	// 		p.curPos = 0
	// 		p.curIndex = 0
	// 	}

	// 	curSpan := p.upperWeights[p.curIndex] - p.curPos
	// 	if leftSpan <= curSpan {
	// 		p.curPos = (p.curPos + leftSpan) % (p.sumWeight + 1)
	// 		leftSpan = 0
	// 	} else {
	// 		p.curPos = p.upperWeights[p.curIndex]
	// 		p.curIndex = (p.curIndex + 1) % len(p.upperWeights)
	// 	}
	// }
	fmt.Printf("curPos = %d, curIndex = %d, step = %d\n", p.curPos, p.curIndex, p.step)

	return sc, nil, nil
}
