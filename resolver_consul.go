package grpclb

import (
	"net"
	"strconv"

	consul_api "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/naming"
)

type consulResolver struct {
	address string
	service string
}

type consulWatcher struct {
	client    *consul_api.Client
	service   string
	addresses map[string]struct{}
	lastIndex uint64
}

func NewResolver(address string, service string) naming.Resolver {
	return &consulResolver{
		address: address,
		service: service,
	}
}

func (r *consulResolver) Resolve(target string) (naming.Watcher, error) {
	config := consul_api.DefaultConfig()
	//config.Address = r.address
	client, err := consul_api.NewClient(config)
	if err != nil {
		return nil, err
	}

	return &consulWatcher{
		client:    client,
		service:   r.service,
		addresses: make(map[string]struct{}),
	}, nil
}

func (w *consulWatcher) Next() ([]*naming.Update, error) {
	for {
		services, metainfo, err := w.client.Health().Service(w.service, "", true, &consul_api.QueryOptions{
			WaitIndex: w.lastIndex,
		})
		if err != nil {
			return nil, err
		}
		w.lastIndex = metainfo.LastIndex

		addrs := make(map[string]struct{})
		for _, s := range services {
			addrs[net.JoinHostPort(s.Service.Address, strconv.Itoa(s.Service.Port))] = struct{}{}
		}

		var updates []*naming.Update
		for addr := range w.addresses {
			if _, ok := addrs[addr]; !ok {
				updates = append(updates, &naming.Update{Op: naming.Delete, Addr: addr})
			}
		}

		for addr := range addrs {
			if _, ok := w.addresses[addr]; !ok {
				updates = append(updates, &naming.Update{Op: naming.Add, Addr: addr})
			}
		}

		if len(updates) != 0 {
			w.addresses = addrs
			return updates, nil
		}
	}
}

func (w *consulWatcher) Close() {
	// nothing to do
}
