package consul

import (
	"fmt"
	"net"
	"time"

	consul_api "github.com/hashicorp/consul/api"
)

type consulRegister struct {
	node_id          string
	consul_addr      string
	service_pre      string
	service_name     string
	tags             []string
	meta             map[string]string
	addr             string
	port             int
	deregister_after time.Duration
	interval         time.Duration
}

func NewRegister(node_id string, consul_addr string, service_pre string, service string, addr string, port int, tags []string, meta_map map[string]string, deregister_after_second uint, interval_second uint) *consulRegister {
	if deregister_after_second <= 0 {
		deregister_after_second = 60
	}

	if interval_second <= 0 {
		interval_second = 30
	}

	return &consulRegister{
		node_id:          node_id,
		consul_addr:      consul_addr,
		service_pre:      service_pre,
		service_name:     service,
		addr:             addr,
		port:             port,
		tags:             tags,
		meta:             meta_map,
		deregister_after: time.Duration(deregister_after_second) * time.Second,
		interval:         time.Duration(interval_second) * time.Second,
	}
}

func (r *consulRegister) Register() error {
	config := consul_api.DefaultConfig()
	config.Address = r.consul_addr
	client, err := consul_api.NewClient(config)
	if err != nil {
		return err
	}
	agent := client.Agent()

	if len(r.addr) <= 0 {
		r.addr = localIP()
	}

	reg := &consul_api.AgentServiceRegistration{
		ID:      r.node_id,
		Name:    fmt.Sprintf("%s.%s", r.service_pre, r.service_name),
		Tags:    r.tags,
		Port:    r.port,
		Address: r.addr,
		Meta:    r.meta,
		Check: &consul_api.AgentServiceCheck{
			Interval:                       r.interval.String(),
			GRPC:                           fmt.Sprintf("%s:%d/%s.%s", r.addr, r.port, r.service_pre, r.service_name),
			DeregisterCriticalServiceAfter: r.deregister_after.String(),
		},
	}

	if err := agent.ServiceRegister(reg); err != nil {
		return err
	}

	return nil
}

func localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
