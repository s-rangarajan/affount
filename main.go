package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

const (
	httpServerAddressEnvVar = "HTTP_ADDRESS"
	shutdownGracePeriod     = 5 * time.Second
)

func main() {
	logger = zap.NewExample().Sugar()
	logger.Info("lesgo")

	dbServer, pool := MustSetupDB()
	// pool := MustSetupRealDB()

	logger.Info("database setup")

	httpServerAddress := MustLoadEnvVar(httpServerAddressEnvVar)

	mainCtx, mainCancel := context.WithCancel(context.Background())

	signalCtx, signalCancel := signal.NotifyContext(mainCtx, os.Interrupt)
	defer signalCancel()

	http.HandleFunc("/health-check", func(w http.ResponseWriter, r *http.Request) {
		pingContext, pingCancel := context.WithTimeout(mainCtx, 100*time.Millisecond)
		defer pingCancel()
		if err := pool.PingContext(pingContext); err != nil {
			logger.Error(err)
			w.WriteHeader(http.StatusInternalServerError)

			return
		}
	})
	http.HandleFunc("/create_account", func(w http.ResponseWriter, r *http.Request) {
		createContext, creationCancel := context.WithTimeout(mainCtx, 100*time.Millisecond)
		defer creationCancel()

		w.Header().Set("Content-Type", "application/json")
		HandleCreateAccountWithContext(createContext, pool, w, r)
	})
	http.HandleFunc("/execute_operations", func(w http.ResponseWriter, r *http.Request) {
		executeContext, executionCancel := context.WithTimeout(mainCtx, 2000*time.Millisecond)
		defer executionCancel()

		w.Header().Set("Content-Type", "application/json")
		HandleExecuteOperationsWithContext(executeContext, pool, w, r)
	})
	http.HandleFunc("/get_account", func(w http.ResponseWriter, r *http.Request) {
		getContext, getCancel := context.WithTimeout(mainCtx, 500*time.Millisecond)
		defer getCancel()

		w.Header().Set("Content-Type", "application/json")
		HandleGetAccountWithContext(getContext, pool, w, r)
	})
	http.HandleFunc("/get_transaction", func(w http.ResponseWriter, r *http.Request) {
		getContext, getCancel := context.WithTimeout(mainCtx, 500*time.Millisecond)
		defer getCancel()

		w.Header().Set("Content-Type", "application/json")
		HandleGetTransactionWithContext(getContext, pool, w, r)
	})

	server := &http.Server{
		ReadTimeout:  5000 * time.Millisecond,
		WriteTimeout: 10000 * time.Millisecond,
		IdleTimeout:  1000 * time.Millisecond,
		Addr:         httpServerAddress,
		Handler:      http.DefaultServeMux,
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Errorf("error cycling server: %w", err)
		}
	}()

	// shutdown signal received
	<-signalCtx.Done()

	// a second before the rug is yanked from under
	// cancel the main context, causing all executing
	// routines, that should respect context to gracefully
	// error out of execution.
	go func() {
		time.Sleep(shutdownGracePeriod - 1*time.Second)
		mainCancel()
	}()

	// start shutdown sequence - no more new requests being served
	shutdownCtx, shutdownCancel := context.WithTimeout(mainCtx, shutdownGracePeriod)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Errorf("error shutting down server: %w", err)
	}

	pool.Close()
	if err := dbServer.Stop(); err != nil {
		logger.Fatal(err)
	}
}

// MustLoadEnvVar takes an input env variable
// and will attempt to load that from the env.
// If it doesn't find it, it will fatally log and exit.
func MustLoadEnvVar(envVar string) string {
	value := os.Getenv(envVar)
	if value == "" {
		panic("missing env var")
	}

	return value
}

func writeHTTPError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)

	errorResponse := struct {
		Errors string `json:"error"`
	}{
		err.Error(),
	}

	b, _ := json.Marshal(errorResponse)
	w.Write(b)
}
