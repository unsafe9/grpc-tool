package tinygrpc

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

type loggingContextKeyType int

const loggingContextKey = loggingContextKeyType(0)

type loggingContextValue struct {
	mu        sync.RWMutex
	keyValues []any
}

func AppendLoggingFields(ctx context.Context, keyValues ...any) {
	if len(keyValues) == 0 {
		return
	}
	if len(keyValues)%2 != 0 {
		panic("AppendLoggingFields requires an even number of key-value pairs")
	}

	ctxValue := ctx.Value(loggingContextKey)
	if ctxValue == nil {
		return
	}
	value := ctxValue.(*loggingContextValue)

	value.mu.Lock()
	defer value.mu.Unlock()
	value.keyValues = append(value.keyValues, keyValues...)
}

func getLoggingFields(ctx context.Context) []any {
	ctxValue := ctx.Value(loggingContextKey)
	if ctxValue == nil {
		return nil
	}
	value := ctxValue.(*loggingContextValue)

	value.mu.RLock()
	defer value.mu.RUnlock()
	fields := make([]any, len(value.keyValues))
	copy(fields, value.keyValues)
	return fields
}

type UnaryLogger interface {
	LogUnaryRequest(c *CallContext, req proto.Message)
	LogUnaryResponse(c *CallContext, duration time.Duration, req, res proto.Message, err error, fields ...any)
}

type StreamLogger interface {
	LogStreamConnect(c *CallContext)
	LogStreamDisconnect(c *CallContext, duration time.Duration, err error, fields ...any)
	LogStreamSendMsg(c *CallContext, message proto.Message, err error)
	LogStreamRecvMsg(c *CallContext, message proto.Message, err error)
}

func UnaryServerLogger(logger UnaryLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = context.WithValue(ctx, loggingContextKey, &loggingContextValue{})
		callCtx := newUnaryCallContext(ctx, info)
		reqMsg, _ := req.(proto.Message)
		logger.LogUnaryRequest(callCtx, reqMsg)

		start := time.Now()
		res, err := handler(ctx, req)
		duration := time.Since(start)

		resMsg, _ := res.(proto.Message)
		logger.LogUnaryResponse(callCtx, duration, reqMsg, resMsg, err, getLoggingFields(ctx)...)
		return res, err
	}
}

func StreamServerLogger(logger StreamLogger, logPayload bool) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := context.WithValue(ss.Context(), loggingContextKey, &loggingContextValue{})
		callCtx := newStreamCallContext(ctx, srv, info)
		logger.LogStreamConnect(callCtx)

		if logPayload {
			ss = &loggingServerStream{
				ServerStream: ss,
				callCtx:      callCtx,
				logger:       logger,
			}
		}

		start := time.Now()
		err := handler(srv, ss)
		duration := time.Since(start)

		logger.LogStreamDisconnect(callCtx, duration, err, getLoggingFields(ctx)...)
		return err
	}
}

type loggingServerStream struct {
	grpc.ServerStream
	callCtx *CallContext
	logger  StreamLogger
}

func (l *loggingServerStream) SendMsg(m any) error {
	err := l.ServerStream.SendMsg(m)
	l.logger.LogStreamSendMsg(l.callCtx, m.(proto.Message), err)
	return err
}

func (l *loggingServerStream) RecvMsg(m any) error {
	err := l.ServerStream.RecvMsg(m)
	l.logger.LogStreamRecvMsg(l.callCtx, m.(proto.Message), err)
	return err
}
