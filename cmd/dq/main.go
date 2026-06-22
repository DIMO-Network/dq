package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/DIMO-Network/dq/internal/app"
	"github.com/DIMO-Network/dq/internal/config"
	"github.com/DIMO-Network/server-garage/pkg/monserver"
	"github.com/DIMO-Network/server-garage/pkg/runner"
	"github.com/DIMO-Network/shared/pkg/settings"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("app", app.AppName).Logger()
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) == 40 {
				logger = logger.With().Str("commit", s.Value[:7]).Logger()
				break
			}
		}
	}
	zerolog.DefaultContextLogger = &logger

	mainCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-mainCtx.Done()
		logger.Info().Msg("Received signal, shutting down...")
		cancel()
	}()

	runnerGroup, runnerCtx := errgroup.WithContext(mainCtx)

	settingsFile := flag.String("settings", "settings.yaml", "settings file")
	flag.Parse()

	cfg, err := settings.LoadConfig[config.Settings](*settingsFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't load settings.")
	}

	if cfg.LogLevel != "" {
		level, err := zerolog.ParseLevel(cfg.LogLevel)
		if err != nil {
			logger.Fatal().Err(err).Str("logLevel", cfg.LogLevel).Msg("Invalid log level.")
		}
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	application, err := app.New(cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't create application.")
	}
	defer application.Cleanup()

	rpcServer, err := app.CreateGRPCServer(&logger, application, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create gRPC server.")
	}

	monSrv := monserver.NewMonitoringServer(&logger, cfg.EnablePprof)
	runner.RunHandler(runnerCtx, runnerGroup, monSrv, ":"+strconv.Itoa(cfg.MonPort))

	mux := http.NewServeMux()
	mux.Handle("/", app.LoggerMiddleware(app.PanicRecoveryMiddleware(playground.Handler("GraphQL playground", "/query"))))
	mux.Handle("/query", application.Handler)
	mux.Handle("/mcp", application.MCPHandler)
	// Real readiness: probes the query backend (catalog reachable + extensions
	// loaded) so a cold pod reports NotReady instead of serving errors (CHD-13).
	mux.HandleFunc("/ready", app.ReadyHandler(application.Ready))

	logger.Info().Msgf("Server started on port: %d", cfg.Port)
	serveHTTP(runnerCtx, runnerGroup, mux, ":"+strconv.Itoa(cfg.Port))

	logger.Info().Msgf("gRPC server started on port: %d", cfg.GRPCPort)
	runner.RunGRPC(runnerCtx, runnerGroup, rpcServer, ":"+strconv.Itoa(cfg.GRPCPort))

	err = runnerGroup.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal().Err(err).Msg("Server shut down due to an error.")
	}
	logger.Info().Msg("Server shut down.")
}

// maxHTTPBodyBytes caps a request body on the public query surface. GraphQL
// queries/variables are small; this blunts a single oversized POST.
const maxHTTPBodyBytes = 4 << 20 // 4 MiB

// serveHTTP runs handler on addr with production timeouts and a body-size cap,
// draining gracefully on ctx cancel via a FRESH bounded context. The vendored
// runner.RunHandler set no timeouts (slowloris / idle exhaustion) and shut down
// with the already-cancelled ctx, which returns immediately and severs in-flight
// requests. No WriteTimeout: GraphQL websocket subscriptions and long lake scans
// are bounded by the per-request timeout middleware, not the socket.
func serveHTTP(ctx context.Context, group *errgroup.Group, handler http.Handler, addr string) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           http.MaxBytesHandler(handler, maxHTTPBodyBytes),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	group.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("failed to run server: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		if err := srv.Shutdown(shCtx); err != nil {
			return fmt.Errorf("failed to shut down server: %w", err)
		}
		return nil
	})
}
