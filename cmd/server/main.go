package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"

	"github.com/atlekbai/schema_registry/internal/config"
	"github.com/atlekbai/schema_registry/internal/db"
	"github.com/atlekbai/schema_registry/internal/handler"
	"github.com/atlekbai/schema_registry/internal/middleware"
	"github.com/atlekbai/schema_registry/internal/schema"
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

	h := handler.New(pool, cache)

	r := mux.NewRouter()
	r.HandleFunc("/api/{object}", h.List).Methods("GET")
	r.HandleFunc("/api/{object}/{id}", h.GetByID).Methods("GET")
	r.Use(middleware.Recovery, middleware.Logging, middleware.ContentType)

	srv := &http.Server{
		Addr:    cfg.Addr(),
		Handler: r,
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
