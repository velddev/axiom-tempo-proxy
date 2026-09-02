package server

import (
	"context"
	"net/http"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/velddev/axiom-tempo-proxy/internal/metrics"
)

// streamingQuerier implements tempopb.StreamingQuerierServer, the gRPC
// service Grafana's Tempo datasource uses when streaming is enabled.
//
// Tempo streams progressive partial results and the client keeps the last
// message it received; a single final message with the complete result is
// therefore valid, and that is what every method here sends.
type streamingQuerier struct {
	s *Server
}

var _ tempopb.StreamingQuerierServer = (*streamingQuerier)(nil)

// prepare resolves the dataset for a call and its schema.
func (g *streamingQuerier) prepare(ctx context.Context) (context.Context, context.CancelFunc, *datasetSchema, error) {
	dataset, err := g.s.grpcDataset(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return g.s.prepare(ctx, dataset)
}

func (g *streamingQuerier) Search(req *tempopb.SearchRequest, stream tempopb.StreamingQuerier_SearchServer) error {
	ctx, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	res, err := g.s.runSearch(ctx, ds, req)
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) SearchTags(req *tempopb.SearchTagsRequest, stream tempopb.StreamingQuerier_SearchTagsServer) error {
	_, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	res, err := g.s.runSearchTags(ds, req.Scope, req.MaxTagsPerScope)
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) SearchTagsV2(req *tempopb.SearchTagsRequest, stream tempopb.StreamingQuerier_SearchTagsV2Server) error {
	_, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	res, err := g.s.runSearchTagsV2(ds, req.Scope, req.MaxTagsPerScope)
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) SearchTagValues(req *tempopb.SearchTagValuesRequest, stream tempopb.StreamingQuerier_SearchTagValuesServer) error {
	ctx, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	start, end := unixSecondsRange(req.Start, req.End)
	res, err := g.s.runSearchTagValues(ctx, ds, req.TagName, req.Query, req.MaxTagValues, start, end)
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) SearchTagValuesV2(req *tempopb.SearchTagValuesRequest, stream tempopb.StreamingQuerier_SearchTagValuesV2Server) error {
	ctx, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	start, end := unixSecondsRange(req.Start, req.End)
	res, err := g.s.runSearchTagValuesV2(ctx, ds, req.TagName, req.Query, req.MaxTagValues, start, end)
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) MetricsQueryRange(req *tempopb.QueryRangeRequest, stream tempopb.StreamingQuerier_MetricsQueryRangeServer) error {
	ctx, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	res, err := g.s.runMetricsQueryRange(ctx, ds, metrics.Request{
		Query:     req.Query,
		StartNs:   req.Start,
		EndNs:     req.End,
		StepNs:    req.Step,
		Exemplars: int(req.Exemplars),
	})
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

func (g *streamingQuerier) MetricsQueryInstant(req *tempopb.QueryInstantRequest, stream tempopb.StreamingQuerier_MetricsQueryInstantServer) error {
	ctx, cancel, ds, err := g.prepare(stream.Context())
	if err != nil {
		return grpcError(err)
	}
	defer cancel()
	res, err := g.s.runMetricsQueryInstant(ctx, ds, metrics.Request{
		Query:   req.Query,
		StartNs: req.Start,
		EndNs:   req.End,
	})
	if err != nil {
		return grpcError(err)
	}
	return stream.Send(res)
}

// unixSecondsRange converts a request's unix-seconds bounds; zero stays
// zero so the caller applies its default window.
func unixSecondsRange(start, end uint32) (time.Time, time.Time) {
	var s, e time.Time
	if start != 0 {
		s = time.Unix(int64(start), 0)
	}
	if end != 0 {
		e = time.Unix(int64(end), 0)
	}
	return s, e
}

// grpcError renders a query-path error as a gRPC status, reusing the same
// classification the HTTP API uses.
func grpcError(err error) error {
	if err == nil {
		return nil
	}
	httpStatus := queryErrorStatus(err)
	if httpStatus == http.StatusGatewayTimeout {
		return status.Error(codes.DeadlineExceeded, "query timed out")
	}
	return status.Error(grpcCode(httpStatus), err.Error())
}

// grpcCode maps an HTTP status to the gRPC code Grafana expects.
func grpcCode(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		// Axiom is unreachable or refused the query.
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	}
	return codes.Internal
}

// logStream logs every gRPC call, mirroring the HTTP request log.
func (s *Server) logStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	s.log.Info("grpc", "method", info.FullMethod, "code", status.Code(err).String(), "duration", time.Since(start))
	return err
}
