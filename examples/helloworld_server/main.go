//go:generate protoc -I ../helloworld --go_out=plugins=grpc:../helloworld ../helloworld/helloworld.proto

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

	"github.com/dodoZeng/grpclb"
	pb "github.com/dodoZeng/grpclb/examples/helloworld"
)

var (
	consul_addr  = "127.0.0.1:8500"
	node_id      = "node.id"
	addr         = "127.0.0.1"
	port         = 50051
	service_pre  = "helloworld"
	service_name = "Greeter"
)

// server is used to implement helloworld.GreeterServer.
type greeter_server struct{}

// SayHello implements helloworld.GreeterServer
func (s *greeter_server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("name: %s, from: %s", in.Name, in.From)
	return &pb.HelloReply{Message: "Hello " + in.Name + "!! [reply from node: " + node_id + "]"}, nil
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
	register := grpclb.NewRegister(node_id, consul_addr, service_pre, service_name, addr, port, nil, 0, 0)
	if err := register.Register(); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return
	}

	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return
	}
}
