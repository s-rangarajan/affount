package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"time"
)

const (
	httpServerAddressEnvVar = "HTTP_ADDRESS"
	shutdownGracePeriod     = 5 * time.Second
)

func main() {
	httpServerAddress := MustLoadEnvVar(httpServerAddressEnvVar)

	mainCtx, mainCancel := context.WithCancel(context.Background())

	signalCtx, signalCancel := signal.NotifyContext(mainCtx, os.Interrupt)
	defer signalCancel()

	http.HandleFunc("/health-check", func(w http.ResponseWriter, r *http.Request) {
		return
	})
	http.HandleFunc("/hold", func(w http.ResponseWriter, r *http.Request) {
		HoldWithContext(mainCtx, haversineEstimator, w, r)
	})
	http.HandleFunc("/update_hold", func(w http.ResponseWriter, r *http.Request) {
		UpdateHoldWithContext(mainCtx, haversineEstimator, w, r)
	})
	http.HandleFunc("/credit", func(w http.ResponseWriter, r *http.Request) {
		CreditWithContext(mainCtx, haversineEstimator, w, r)
	})
	http.HandleFunc("/capture", func(w http.ResponseWriter, r *http.Request) {
		CaptureWithContext(mainCtx, haversineEstimator, w, r)
	})
	http.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		ReleaseWithContext(mainCtx, haversineEstimator, w, r)
	})

	server := &http.Server{Addr: httpServerAddress, Handler: http.DefaultServeMux}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			panic("error closing listeners")
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
		panic(err)
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
