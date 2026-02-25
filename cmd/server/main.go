package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"connectrpc.com/vanguard"

	"github.com/atlekbai/schema_registry/internal/config"
	"github.com/atlekbai/schema_registry/internal/db"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/atlekbai/schema_registry/internal/server"
	"github.com/atlekbai/schema_registry/internal/service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	cache := schema.NewCache()
	if err := cache.Load(ctx, pool); err != nil {
		log.Fatalf("failed to load schema cache: %v", err)
	}
	log.Printf("schema cache loaded: %d objects", cache.ObjectCount())

	validator, err := protovalidate.New()
	if err != nil {
		log.Fatalf("failed to create validator: %v", err)
	}

	interceptors := []connect.Interceptor{
		server.ValidationInterceptor(validator),
	}

	services := []server.ConnectService{
		service.NewRegistryService(pool, cache),
	}

	vanguardServices := make([]*vanguard.Service, len(services))
	for i, svc := range services {
		path, handler := svc.RegisterHandler(interceptors...)
		vanguardServices[i] = vanguard.NewService(path, handler)
	}

	// Vanguard transcodes REST (google.api.http annotations) to Connect/gRPC.
	transcoder, err := vanguard.NewTranscoder(vanguardServices)
	if err != nil {
		log.Fatalf("vanguard transcoder: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", transcoder)

	srv := &http.Server{
		Addr:    cfg.Addr(),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		srv.Shutdown(context.Background())
	}()

	log.Printf("listening on %s", cfg.Addr())
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
