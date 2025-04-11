// Generated by j5build v0.0.0-20250403212908-de7c3c2e6cce. DO NOT EDIT

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: o5/ges/v1/service/events.p.j5s.proto

package ges_spb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	QueryService_EventsList_FullMethodName = "/o5.ges.v1.service.QueryService/EventsList"
	QueryService_UpsertList_FullMethodName = "/o5.ges.v1.service.QueryService/UpsertList"
)

// QueryServiceClient is the client API for QueryService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type QueryServiceClient interface {
	EventsList(ctx context.Context, in *EventsListRequest, opts ...grpc.CallOption) (*EventsListResponse, error)
	UpsertList(ctx context.Context, in *UpsertListRequest, opts ...grpc.CallOption) (*UpsertListResponse, error)
}

type queryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewQueryServiceClient(cc grpc.ClientConnInterface) QueryServiceClient {
	return &queryServiceClient{cc}
}

func (c *queryServiceClient) EventsList(ctx context.Context, in *EventsListRequest, opts ...grpc.CallOption) (*EventsListResponse, error) {
	out := new(EventsListResponse)
	err := c.cc.Invoke(ctx, QueryService_EventsList_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryServiceClient) UpsertList(ctx context.Context, in *UpsertListRequest, opts ...grpc.CallOption) (*UpsertListResponse, error) {
	out := new(UpsertListResponse)
	err := c.cc.Invoke(ctx, QueryService_UpsertList_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QueryServiceServer is the server API for QueryService service.
// All implementations must embed UnimplementedQueryServiceServer
// for forward compatibility
type QueryServiceServer interface {
	EventsList(context.Context, *EventsListRequest) (*EventsListResponse, error)
	UpsertList(context.Context, *UpsertListRequest) (*UpsertListResponse, error)
	mustEmbedUnimplementedQueryServiceServer()
}

// UnimplementedQueryServiceServer must be embedded to have forward compatible implementations.
type UnimplementedQueryServiceServer struct {
}

func (UnimplementedQueryServiceServer) EventsList(context.Context, *EventsListRequest) (*EventsListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EventsList not implemented")
}
func (UnimplementedQueryServiceServer) UpsertList(context.Context, *UpsertListRequest) (*UpsertListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpsertList not implemented")
}
func (UnimplementedQueryServiceServer) mustEmbedUnimplementedQueryServiceServer() {}

// UnsafeQueryServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to QueryServiceServer will
// result in compilation errors.
type UnsafeQueryServiceServer interface {
	mustEmbedUnimplementedQueryServiceServer()
}

func RegisterQueryServiceServer(s grpc.ServiceRegistrar, srv QueryServiceServer) {
	s.RegisterService(&QueryService_ServiceDesc, srv)
}

func _QueryService_EventsList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EventsListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServiceServer).EventsList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueryService_EventsList_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServiceServer).EventsList(ctx, req.(*EventsListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _QueryService_UpsertList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpsertListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServiceServer).UpsertList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: QueryService_UpsertList_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServiceServer).UpsertList(ctx, req.(*UpsertListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// QueryService_ServiceDesc is the grpc.ServiceDesc for QueryService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var QueryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "o5.ges.v1.service.QueryService",
	HandlerType: (*QueryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "EventsList",
			Handler:    _QueryService_EventsList_Handler,
		},
		{
			MethodName: "UpsertList",
			Handler:    _QueryService_UpsertList_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "o5/ges/v1/service/events.p.j5s.proto",
}

const (
	CommandService_ReplayEvents_FullMethodName  = "/o5.ges.v1.service.CommandService/ReplayEvents"
	CommandService_ReplayUpserts_FullMethodName = "/o5.ges.v1.service.CommandService/ReplayUpserts"
)

// CommandServiceClient is the client API for CommandService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CommandServiceClient interface {
	ReplayEvents(ctx context.Context, in *ReplayEventsRequest, opts ...grpc.CallOption) (*ReplayEventsResponse, error)
	ReplayUpserts(ctx context.Context, in *ReplayUpsertsRequest, opts ...grpc.CallOption) (*ReplayUpsertsResponse, error)
}

type commandServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewCommandServiceClient(cc grpc.ClientConnInterface) CommandServiceClient {
	return &commandServiceClient{cc}
}

func (c *commandServiceClient) ReplayEvents(ctx context.Context, in *ReplayEventsRequest, opts ...grpc.CallOption) (*ReplayEventsResponse, error) {
	out := new(ReplayEventsResponse)
	err := c.cc.Invoke(ctx, CommandService_ReplayEvents_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *commandServiceClient) ReplayUpserts(ctx context.Context, in *ReplayUpsertsRequest, opts ...grpc.CallOption) (*ReplayUpsertsResponse, error) {
	out := new(ReplayUpsertsResponse)
	err := c.cc.Invoke(ctx, CommandService_ReplayUpserts_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CommandServiceServer is the server API for CommandService service.
// All implementations must embed UnimplementedCommandServiceServer
// for forward compatibility
type CommandServiceServer interface {
	ReplayEvents(context.Context, *ReplayEventsRequest) (*ReplayEventsResponse, error)
	ReplayUpserts(context.Context, *ReplayUpsertsRequest) (*ReplayUpsertsResponse, error)
	mustEmbedUnimplementedCommandServiceServer()
}

// UnimplementedCommandServiceServer must be embedded to have forward compatible implementations.
type UnimplementedCommandServiceServer struct {
}

func (UnimplementedCommandServiceServer) ReplayEvents(context.Context, *ReplayEventsRequest) (*ReplayEventsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplayEvents not implemented")
}
func (UnimplementedCommandServiceServer) ReplayUpserts(context.Context, *ReplayUpsertsRequest) (*ReplayUpsertsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReplayUpserts not implemented")
}
func (UnimplementedCommandServiceServer) mustEmbedUnimplementedCommandServiceServer() {}

// UnsafeCommandServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CommandServiceServer will
// result in compilation errors.
type UnsafeCommandServiceServer interface {
	mustEmbedUnimplementedCommandServiceServer()
}

func RegisterCommandServiceServer(s grpc.ServiceRegistrar, srv CommandServiceServer) {
	s.RegisterService(&CommandService_ServiceDesc, srv)
}

func _CommandService_ReplayEvents_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplayEventsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CommandServiceServer).ReplayEvents(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: CommandService_ReplayEvents_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CommandServiceServer).ReplayEvents(ctx, req.(*ReplayEventsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CommandService_ReplayUpserts_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplayUpsertsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CommandServiceServer).ReplayUpserts(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: CommandService_ReplayUpserts_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CommandServiceServer).ReplayUpserts(ctx, req.(*ReplayUpsertsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// CommandService_ServiceDesc is the grpc.ServiceDesc for CommandService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CommandService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "o5.ges.v1.service.CommandService",
	HandlerType: (*CommandServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReplayEvents",
			Handler:    _CommandService_ReplayEvents_Handler,
		},
		{
			MethodName: "ReplayUpserts",
			Handler:    _CommandService_ReplayUpserts_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "o5/ges/v1/service/events.p.j5s.proto",
}
