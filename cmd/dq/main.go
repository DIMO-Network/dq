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
	// Backfill mode (finding #1a): a one-shot re-decode of a raw_events time range into
	// the decoded lake tables, for a range the decode loop permanently skipped on cursor
	// expiry (DQMaterializerCursorReset). Both flags RFC3339; setting either runs the
	// backfill and exits instead of starting the servers/decode loop. Idempotent.
	backfillFrom := flag.String("backfill-from", "", "RFC3339 start (inclusive) of a raw_events range to re-decode, then exit")
	backfillTo := flag.String("backfill-to", "", "RFC3339 end (exclusive) of a raw_events range to re-decode, then exit")
	flag.Parse()

	cfg, err := settings.LoadConfig[config.Settings](*settingsFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't load settings.")
	}

	if *backfillFrom != "" || *backfillTo != "" {
		runBackfillAndExit(cfg, *backfillFrom, *backfillTo, logger)
		return
	}
	// The shared env loader silently leaves a field zero on a malformed value (it swallows
	// per-field parse errors), so boot-critical numerics never fail LoadConfig — validate
	// them loud here (a zero port black-holes traffic; a zero chain id mis-decodes DIDs).
	if err := cfg.Validate(); err != nil {
		logger.Fatal().Err(err).Msg("Invalid configuration.")
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
	serveHTTP(runnerCtx, runnerGroup, logger, monSrv, ":"+strconv.Itoa(cfg.MonPort))

	mux := http.NewServeMux()
	mux.Handle("/", app.LoggerMiddleware(app.PanicRecoveryMiddleware(playground.Handler("GraphQL playground", "/query"))))
	mux.Handle("/query", application.Handler)
	mux.Handle("/mcp", application.MCPHandler)
	// Real readiness: probes the query backend (catalog reachable + extensions
	// loaded) so a cold pod reports NotReady instead of serving errors (CHD-13).
	mux.HandleFunc("/ready", app.ReadyHandler(application.Ready))

	logger.Info().Msgf("Server started on port: %d", cfg.Port)
	serveHTTP(runnerCtx, runnerGroup, logger, mux, ":"+strconv.Itoa(cfg.Port))

	logger.Info().Msgf("gRPC server started on port: %d", cfg.GRPCPort)
	runner.RunGRPC(runnerCtx, runnerGroup, rpcServer, ":"+strconv.Itoa(cfg.GRPCPort))

	err = runnerGroup.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal().Err(err).Msg("Server shut down due to an error.")
	}
	logger.Info().Msg("Server shut down.")
}

// runBackfillAndExit parses the RFC3339 backfill window, runs the one-shot re-decode
// (finding #1a), and exits. Both bounds are required and from must precede to.
func runBackfillAndExit(cfg config.Settings, fromStr, toStr string, logger zerolog.Logger) {
	if fromStr == "" || toStr == "" {
		logger.Fatal().Msg("backfill requires both -backfill-from and -backfill-to (RFC3339)")
	}
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		logger.Fatal().Err(err).Str("backfill-from", fromStr).Msg("invalid -backfill-from (want RFC3339)")
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		logger.Fatal().Err(err).Str("backfill-to", toStr).Msg("invalid -backfill-to (want RFC3339)")
	}
	if err := app.RunBackfill(cfg, from, to, logger); err != nil {
		logger.Fatal().Err(err).Msg("backfill failed")
	}
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
func serveHTTP(ctx context.Context, group *errgroup.Group, log zerolog.Logger, handler http.Handler, addr string) {
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
			// A slow drain hitting the deadline on a normal SIGTERM must not become
			// a non-zero exit (the errgroup would Fatal); log it and exit cleanly.
			log.Warn().Err(err).Str("addr", addr).Msg("http server did not drain within the shutdown deadline")
		}
		return nil
	})
}
