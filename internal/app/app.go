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
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	fetchgrpc "github.com/DIMO-Network/dq/pkg/grpc"
	"github.com/DIMO-Network/server-garage/pkg/gql/errorhandler"
	gqlmetrics "github.com/DIMO-Network/server-garage/pkg/gql/metrics"
	"github.com/DIMO-Network/server-garage/pkg/mcpserver"
	"github.com/DIMO-Network/shared/pkg/middleware/metrics"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// AppName is the name of the application.
var AppName = "dq"

// App is the main application.
type App struct {
	Handler    http.Handler
	MCPHandler http.Handler
	cleanup    func()
}

// New creates a new application.
func New(settings config.Settings) (*App, error) {
	chService, err := ch.NewService(settings)
	if err != nil {
		return nil, fmt.Errorf("couldn't create ClickHouse service: %w", err)
	}
	signalRepo, err := repositories.NewRepository(chService)
	if err != nil {
		return nil, fmt.Errorf("couldn't create signal repository: %w", err)
	}

	chConn, err := chClientFromSettings(&settings.ClickhouseFileCatalogue)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection for event repo: %w", err)
	}
	s3Client := s3ClientFromSettings(&settings)
	eventService := eventrepo.New(chConn, s3Client, s3.NewPresignClient(s3Client), settings.ParquetBucket, settings.BlobBucket)

	buckets := []string{settings.CloudEventBucket, settings.EphemeralBucket, settings.ParquetBucket}

	var identityClient identity.Client
	if settings.IdentityAPIURL != "" {
		identityClient = identity.New(settings.IdentityAPIURL)
	}

	resolver := &graph.Resolver{
		SignalRepo:      signalRepo,
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

	return &App{
		Handler:    authChain(gqlSrv),
		MCPHandler: authChain(mcpHandler),
		cleanup:    func() {},
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
func CreateGRPCServer(logger *zerolog.Logger, settings *config.Settings) (*grpc.Server, error) {
	chConn, err := chClientFromSettings(&settings.ClickhouseFileCatalogue)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}

	s3Client := s3ClientFromSettings(settings)
	eventService := eventrepo.New(chConn, s3Client, s3.NewPresignClient(s3Client), settings.ParquetBucket, settings.BlobBucket)

	buckets := []string{settings.CloudEventBucket, settings.EphemeralBucket, settings.ParquetBucket}
	rpcServer := rpc.NewServer(buckets, eventService)

	grpcPanic := metrics.GRPCPanicker{Logger: logger}
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metrics.GRPCMetricsAndLogMiddleware(logger),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanic.GRPCPanicRecoveryHandler)),
		),
	)
	fetchgrpc.RegisterFetchServiceServer(server, rpcServer)

	return server, nil
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
