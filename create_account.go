package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
)

type createAccountRequest struct {
	UserARI string `json:"user_ari"`
}

func HandleCreateAccountWithContext(ctx context.Context, pool *sql.DB, w http.ResponseWriter, r *http.Request) {
	defer logger.Sync()
	logger.Info("received create account request")
	if r.Body == nil {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error empty request body"))
		return
	}

	var req createAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, http.StatusUnprocessableEntity, fmt.Errorf("error decoding request body: %w", err))
		return
	}

	if req.UserARI == "" {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error missing required fields"))
		return
	}

	logger.Infow("handling create account request", "request", req)
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error beginning transaction: %w", err))
		debug.PrintStack()
		return
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			logger.Errorf("error cleaning up transaction: %s", err.Error())
		}
	}()

	account, err := CreateAccountWithContext(ctx, tx, req.UserARI)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error executing database operations: %w", err))
		debug.PrintStack()
		return
	}

	if err := tx.Commit(); err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error committing database state: %w", err))
		debug.PrintStack()
		return
	}

	marshaledAccount, err := json.Marshal(account)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
		debug.PrintStack()
		return
	}
	logger.Infow("account created", "request", req, "account", account)

	w.WriteHeader(http.StatusOK)
	w.Write(marshaledAccount)
}
