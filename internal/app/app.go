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
	"github.com/DIMO-Network/dq/pkg/eventrepo"
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
	// ready (e.g. a backend with no health probe).
	readyCheck func(context.Context) error
	// eventService is the query backend New already built; the gRPC server reuses
	// it instead of opening a second duck.Service + S3 client in the same process
	// (SR-9). Owned by this App — Cleanup closes it.
	eventService eventrepo.EventService
	// identityClient (may be nil) lets the gRPC fetch server scope reads to the
	// token's own subject + verify cross-subject device links, the same way the
	// GraphQL resolver does. Shared with the resolver.
	identityClient identity.Client
	// globalLimiter is the process-wide in-flight cap; the gRPC server shares
	// it with the HTTP chain so both transports draw from ONE admission budget
	// in front of the one DuckDB pool (H11).
	globalLimiter *limits.GlobalLimiter
}

// New creates a new application.
func New(settings config.Settings) (*App, error) {
	logger := appLogger()

	backend, duckSvc, backendCleanup, err := newQueryBackend(&settings, logger)
	if err != nil {
		return nil, err
	}
	// Past this point a failed boot must release the DuckDB service — and the
	// materializer loop once started — or it leaks the catalog connection and a
	// running goroutine. One deferred cleanup-on-error covers every return below, so
	// a newly added one can't silently forget it; cleared (ok=true) only once the App
	// takes ownership of the cleanup.
	var stopMaterializer func()
	cleanup := func() {
		if stopMaterializer != nil {
			stopMaterializer()
		}
		backendCleanup()
	}
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()

	signalRepo, err := repositories.NewRepository(backend)
	if err != nil {
		return nil, fmt.Errorf("couldn't create signal repository: %w", err)
	}

	if settings.MaterializerEnabled {
		stopMaterializer, err = startMaterializer(&settings, logger)
		if err != nil {
			return nil, fmt.Errorf("couldn't start materializer: %w", err)
		}
	}

	s3Client := s3ClientFromSettings(&settings)
	eventService, err := newEventService(&settings, duckSvc, s3Client, logger)
	if err != nil {
		return nil, fmt.Errorf("couldn't create event service: %w", err)
	}

	var identityClient identity.Client
	if settings.IdentityAPIURL != "" {
		identityClient = identity.New(settings.IdentityAPIURL)
	}

	resolver := &graph.Resolver{
		SignalRepo:     signalRepo,
		EventService:   eventService,
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

	concLimiter := limits.NewConcurrencyLimiter(settings.MaxConcurrentPerSubject)
	globalLimiter := limits.NewGlobalLimiter(settings.MaxConcurrentRequests)
	// subjectKey limits by the authenticated JWT subject (the developer/caller). It is
	// applied just inside CheckJWT, which has populated the claims by that point.
	subjectKey := func(r *http.Request) string {
		if claims, ok := auth.GetValidatedClaims(r.Context()); ok {
			return claims.RegisteredClaims.Subject
		}
		return ""
	}

	authChain := func(inner http.Handler) http.Handler {
		return PanicRecoveryMiddleware(
			LoggerMiddleware(
				// Global admission first (H11): shed load BEFORE spending JWT
				// validation and before a request can queue on the DuckDB pool.
				globalLimiter.Middleware(
					limiter.AddRequestTimeout(
						jwtMiddleware.CheckJWT(
							concLimiter.Middleware(subjectKey)(
								authLoggerMiddleware(
									auth.AddClaimHandler(inner),
								),
							),
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
		// Wrap so query load can't flip a healthy pod to NotReady and cascade (CHD review).
		readyCheck = loadTolerantReadiness(duckReadiness(duckSvc), readyCacheTTL, readyGraceWindow)
	}

	ok = true // App now owns backendCleanup + stopMaterializer; disarm the deferred cleanup
	return &App{
		Handler:        authChain(gqlSrv),
		MCPHandler:     authChain(mcpHandler),
		readyCheck:     readyCheck,
		eventService:   eventService,
		identityClient: identityClient,
		globalLimiter:  globalLimiter,
		cleanup:        cleanup,
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

// CreateGRPCServer builds the fetch gRPC server, reusing the query backend the
// given App already opened rather than opening a second duck.Service + S3 client
// in the same process (SR-9). The App owns that backend, so there is no separate
// cleanup to return.
func CreateGRPCServer(logger *zerolog.Logger, application *App, settings config.Settings) (*grpc.Server, error) {
	rpcServer := rpc.NewServer(application.eventService, application.identityClient)

	grpcPanic := metrics.GRPCPanicker{Logger: logger}
	interceptors := []grpc.UnaryServerInterceptor{
		metrics.GRPCMetricsAndLogMiddleware(logger),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanic.GRPCPanicRecoveryHandler)),
		// Process-wide admission (shared with the HTTP chain): the fetch port
		// had no concurrency bound at all, so blob-heavy RPC bursts queued
		// unboundedly on the DuckDB pool + Go heap (H11). After metrics so
		// rejections are observable, before auth so shed requests stay cheap.
		application.globalLimiter.UnaryInterceptor(),
		// Bound a fetch RPC that arrives with no client deadline: DuckDB cancels
		// via context, so without a deadline a pathological query runs unbounded on
		// one of the few pooled connections and can starve the replica.
		unaryDeadlineInterceptor(defaultFetchRPCTimeout),
	}
	// Authenticate the fetch port with a DIMO JWT when an issuer is configured —
	// the RPCs return any subject's raw data, so an unauthenticated port is
	// readable by any in-cluster caller. Added after metrics/recovery so auth
	// rejections are still logged and a panic in validation is contained.
	if settings.TokenExchangeIssuer != "" {
		authInterceptor, err := auth.NewGRPCFetchAuthInterceptor(
			settings.TokenExchangeIssuer, settings.TokenExchangeJWTKeySetURL, settings.FetchGRPCRequireJWT, logger)
		if err != nil {
			return nil, fmt.Errorf("creating fetch gRPC auth interceptor: %w", err)
		}
		interceptors = append(interceptors, authInterceptor)
	} else {
		logger.Warn().Msg("fetch gRPC has no JWT auth (TOKEN_EXCHANGE_ISSUER_URL unset) — relying on the network policy only")
	}
	server := grpc.NewServer(
		// Blob payloads run 4–50 MiB; the gRPC default 4 MiB send limit
		// silently truncated them once the fetch path started serving blobs
		// (CHD-22). Raise both directions to cover the largest payloads.
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.ChainUnaryInterceptor(interceptors...),
	)
	fetchgrpc.RegisterFetchServiceServer(server, rpcServer)

	return server, nil
}

// defaultFetchRPCTimeout bounds a fetch RPC that arrives without a client
// deadline. Generous (blob downloads + lake scans) but finite.
const defaultFetchRPCTimeout = 30 * time.Second

// unaryDeadlineInterceptor applies d as a deadline to any request whose context
// has none, so DuckDB query cancellation always has something to fire on.
func unaryDeadlineInterceptor(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return handler(ctx, req)
	}
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
