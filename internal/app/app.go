package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/DIMO-Network/dq/internal/auth"
	"github.com/DIMO-Network/dq/internal/config"
	"github.com/DIMO-Network/dq/internal/fetch/rpc"
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/identity"
	"github.com/DIMO-Network/dq/internal/limits"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/ch"
	fetchgrpc "github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/DIMO-Network/server-garage/pkg/gql/errorhandler"
	gqlmetrics "github.com/DIMO-Network/server-garage/pkg/gql/metrics"
	"github.com/DIMO-Network/server-garage/pkg/mcpserver"
	"github.com/DIMO-Network/shared/pkg/middleware/metrics"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// AppName is the name of the application.
var AppName = "dq"

// maxGRPCMessageBytes bounds gRPC message size. Cloudevent blob payloads reach
// ~50 MiB; the 4 MiB default would truncate them on the fetch path (CHD-22).
const maxGRPCMessageBytes = 50 << 20 // 50 MiB

// App is the main application.
type App struct {
	Handler    http.Handler
	MCPHandler http.Handler
	cleanup    func()
	// readyCheck probes backend health for the /ready endpoint; nil = always
	// ready (e.g. pure ClickHouse mode).
	readyCheck func(context.Context) error
}

// New creates a new application.
func New(settings config.Settings) (*App, error) {
	logger := appLogger()

	// chService is only needed for clickhouse/duckdb/shadow backends. In
	// ducklake mode we skip constructing any ClickHouse client entirely.
	var chService *ch.Service
	if settings.QueryBackend != config.QueryBackendDuckLake {
		var err error
		chService, err = ch.NewService(settings)
		if err != nil {
			return nil, fmt.Errorf("couldn't create ClickHouse service: %w", err)
		}
	}

	backend, duckSvc, backendCleanup, err := newQueryBackend(&settings, chService, logger)
	if err != nil {
		return nil, err
	}
	signalRepo, err := repositories.NewRepository(backend)
	if err != nil {
		backendCleanup()
		return nil, fmt.Errorf("couldn't create signal repository: %w", err)
	}

	var stopMaterializer func()
	if settings.MaterializerEnabled {
		stopMaterializer, err = startMaterializer(&settings, logger)
		if err != nil {
			backendCleanup()
			return nil, fmt.Errorf("couldn't start materializer: %w", err)
		}
	}

	s3Client := s3ClientFromSettings(&settings)
	eventService, err := newEventService(&settings, duckSvc, s3Client)
	if err != nil {
		if stopMaterializer != nil {
			stopMaterializer()
		}
		backendCleanup()
		return nil, fmt.Errorf("couldn't create event service: %w", err)
	}

	buckets := []string{settings.CloudEventBucket, settings.EphemeralBucket, settings.ParquetBucket}

	var identityClient identity.Client
	if settings.IdentityAPIURL != "" {
		identityClient = identity.New(settings.IdentityAPIURL)
	}

	resolver := &graph.Resolver{
		SignalRepo:     signalRepo,
		EventService:   eventService,
		Buckets:        buckets,
		IdentityClient: identityClient,
	}

	cfg := graph.Config{Resolvers: resolver}
	cfg.Directives.RequiresVehicleToken = auth.NewVehicleTokenCheck()
	cfg.Directives.RequiresAllOfPrivileges = auth.AllOfPrivilegeCheck
	cfg.Directives.RequiresOneOfPrivilege = auth.OneOfPrivilegeCheck
	cfg.Directives.IsSignal = noOp
	cfg.Directives.HasAggregation = noOp

	es := graph.NewExecutableSchema(cfg)
	gqlSrv := newServer(es)

	jwtMiddleware, err := auth.NewJWTMiddleware(settings.TokenExchangeIssuer, settings.TokenExchangeJWTKeySetURL)
	if err != nil {
		return nil, fmt.Errorf("couldn't create JWT middleware: %w", err)
	}

	limiter, err := limits.New(settings.MaxRequestDuration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create request time limit middleware: %w", err)
	}

	authChain := func(inner http.Handler) http.Handler {
		return PanicRecoveryMiddleware(
			LoggerMiddleware(
				limiter.AddRequestTimeout(
					jwtMiddleware.CheckJWT(
						authLoggerMiddleware(
							auth.AddClaimHandler(inner),
						),
					),
				),
			),
		)
	}

	mcpHandler, err := mcpserver.New(
		mcpserver.NewGQLGenExecutor(es),
		"DIMO Query", "0.1.0", "dq",
		mcpserver.WithTools(graph.MCPTools),
		mcpserver.WithCondensedSchema(graph.CondensedSchema),
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't create MCP handler: %w", err)
	}

	var readyCheck func(context.Context) error
	if duckSvc != nil {
		readyCheck = duckReadiness(duckSvc, settings.QueryBackend)
	}

	return &App{
		Handler:    authChain(gqlSrv),
		MCPHandler: authChain(mcpHandler),
		readyCheck: readyCheck,
		cleanup: func() {
			if stopMaterializer != nil {
				stopMaterializer()
			}
			backendCleanup()
		},
	}, nil
}

// Cleanup runs any cleanup logic.
func (a *App) Cleanup() {
	if a.cleanup != nil {
		a.cleanup()
	}
}

func noOp(ctx context.Context, obj interface{}, next graphql.Resolver) (res interface{}, err error) {
	return next(ctx)
}

// CreateGRPCServer creates a new gRPC server wired to the event service.
// In ducklake mode no ClickHouse client is constructed.
// The returned cleanup function releases the underlying duck.Service (DuckDB
// connection + catalog attach) and must be called after the server has stopped
// serving (e.g. after GracefulStop returns).
func CreateGRPCServer(logger *zerolog.Logger, settings *config.Settings) (*grpc.Server, func(), error) {
	// Build a duck.Service for backends that need the catalog.
	// chService is nil in ducklake mode; newQueryBackend handles that.
	var chSvc *ch.Service
	if settings.QueryBackend != config.QueryBackendDuckLake {
		var err error
		chSvc, err = ch.NewService(*settings)
		if err != nil {
			return nil, nil, fmt.Errorf("creating ClickHouse service for gRPC: %w", err)
		}
	}

	_, duckSvc, cleanup, err := newQueryBackend(settings, chSvc, *logger)
	if err != nil {
		return nil, nil, fmt.Errorf("creating query backend for gRPC: %w", err)
	}

	s3Client := s3ClientFromSettings(settings)
	eventService, err := newEventService(settings, duckSvc, s3Client)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("creating event service for gRPC: %w", err)
	}

	buckets := []string{settings.CloudEventBucket, settings.EphemeralBucket, settings.ParquetBucket}
	rpcServer := rpc.NewServer(buckets, eventService)

	grpcPanic := metrics.GRPCPanicker{Logger: logger}
	server := grpc.NewServer(
		// Blob payloads run 4–50 MiB; the gRPC default 4 MiB send limit
		// silently truncated them once the fetch path started serving blobs
		// (CHD-22). Raise both directions to cover the largest payloads.
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.ChainUnaryInterceptor(
			metrics.GRPCMetricsAndLogMiddleware(logger),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanic.GRPCPanicRecoveryHandler)),
		),
	)
	fetchgrpc.RegisterFetchServiceServer(server, rpcServer)

	return server, cleanup, nil
}

func newServer(es graphql.ExecutableSchema) *handler.Server {
	srv := handler.New(es)

	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
	})
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.MultipartForm{})
	srv.Use(extension.FixedComplexityLimit(100))
	srv.Use(extension.Introspection{})
	srv.Use(gqlmetrics.Tracer{})
	srv.SetErrorPresenter(errorhandler.ErrorPresenter)

	return srv
}
