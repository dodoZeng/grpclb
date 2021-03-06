//go:generate protoc -I ../helloworld --go_out=plugins=grpc:../helloworld ../helloworld/helloworld.proto

// consul agent -server -ui -bootstrap -data-dir=/tmp/consul -node=n1 -bind=0.0.0.0 -client=0.0.0.0

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/dodoZeng/grpclb/examples/helloworld"
	"github.com/dodoZeng/grpclb/resolver/consul"
)

var (
	consul_addr  = "127.0.0.1:8500"
	node_id      = "node.id"
	addr         = "127.0.0.1"
	port         = 50051
	service_pre  = "helloworld"
	service_name = "Greeter"
	weight       = "1"
	hash         = "10"
)

// server is used to implement helloworld.GreeterServer.
type greeter_server struct{}

// SayHello implements helloworld.GreeterServer
func (s *greeter_server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	//time.Sleep(time.Millisecond * 200)
	return &pb.HelloReply{Message: "Hello " + in.Name + "! [reply from node: " + node_id + "]"}, nil
}

// 实现健康检查接口，提供给consul调用(也可以在注册时使用其他健康检查方式，在这里是使用grpc的健康检查)
type health_server struct{}

func (h *health_server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (h *health_server) Watch(*grpc_health_v1.HealthCheckRequest, grpc_health_v1.Health_WatchServer) error {
	return nil
}

func init() {
	if len(os.Args) > 1 {
		node_id = os.Args[1]
	}
	if len(os.Args) > 2 {
		port, _ = strconv.Atoi(os.Args[2])
	}
	if len(os.Args) > 3 {
		weight = os.Args[3]
	}
	if len(os.Args) > 4 {
		hash = os.Args[4]
	}

}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &greeter_server{})
	grpc_health_v1.RegisterHealthServer(s, &health_server{})

	// 注册服务到consul
	serviceMeta := map[string]string{
		"hash":   hash,   // the hash of the service node
		"weight": weight, // the weight of the service node
	}
	register := consul.NewRegister(node_id, consul_addr, service_pre, service_name, addr, port, nil, serviceMeta, 0, 0)
	if err := register.Register(); err != nil {
		log.Fatalf("failed to register: %v", err)
		return
	}

	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return
	}
}
