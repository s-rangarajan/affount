package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
)

func HandleGetAccountWithContext(ctx context.Context, pool *sql.DB, w http.ResponseWriter, r *http.Request) {
	defer logger.Sync()
	logger.Info("received get account request")
	accountID, err := strconv.ParseUint(r.URL.Query().Get("account_id"), 10, 64)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error missing/invalid account_id parameter"))
		return
	}

	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		logger.Errorf("error beginning get account transaction: %s", err.Error())
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error beginning transaction: %w", err))
		return
	}
	defer func() {
		tx.Rollback()
	}()

	logger.Infow("handling get account request", "account_id", accountID)
	account, err := GetAccountWithContext(ctx, tx, accountID)
	if err != nil {
		logger.Errorf("error executing get account database operations: %s", err.Error())
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error executing database operations: %w", err))
		debug.PrintStack()
		return
	}

	if err := tx.Commit(); err != nil {
		logger.Errorf("error committing get account transaction: %s", err.Error())
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error committing database state: %w", err))
		debug.PrintStack()
		return
	}

	marshaledAccount, err := json.Marshal(account)
	if err != nil {
		logger.Errorf("error marshaling get account response: %s", err.Error())
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
		debug.PrintStack()
		return
	}
	logger.Infow("account fetched", "account_id", accountID, "account", account)

	w.WriteHeader(http.StatusOK)
	w.Write(marshaledAccount)
}
