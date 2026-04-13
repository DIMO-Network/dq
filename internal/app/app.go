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
	"github.com/DIMO-Network/dq/internal/graph"
	"github.com/DIMO-Network/dq/internal/identity"
	"github.com/DIMO-Network/dq/internal/limits"
	"github.com/DIMO-Network/dq/internal/repositories"
	"github.com/DIMO-Network/dq/internal/service/ch"
	"github.com/DIMO-Network/dq/pkg/eventrepo"
	"github.com/DIMO-Network/server-garage/pkg/gql/errorhandler"
	gqlmetrics "github.com/DIMO-Network/server-garage/pkg/gql/metrics"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// AppName is the name of the application.
var AppName = "dq"

// App is the main application.
type App struct {
	Handler http.Handler
	cleanup func()
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

	chConn, err := chClientFromSettings(&settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection for event repo: %w", err)
	}
	s3Client := s3ClientFromSettings(&settings)
	eventService := eventrepo.New(chConn, s3Client, s3.NewPresignClient(s3Client), settings.ParquetBucket)

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

	gqlSrv := newServer(graph.NewExecutableSchema(cfg))

	jwtMiddleware, err := auth.NewJWTMiddleware(settings.TokenExchangeIssuer, settings.TokenExchangeJWTKeySetURL)
	if err != nil {
		return nil, fmt.Errorf("couldn't create JWT middleware: %w", err)
	}

	limiter, err := limits.New(settings.MaxRequestDuration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create request time limit middleware: %w", err)
	}

	serverHandler := PanicRecoveryMiddleware(
		LoggerMiddleware(
			limiter.AddRequestTimeout(
				jwtMiddleware.CheckJWT(
					authLoggerMiddleware(
						auth.AddClaimHandler(gqlSrv),
					),
				),
			),
		),
	)

	return &App{
		Handler: serverHandler,
		cleanup: func() {},
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
