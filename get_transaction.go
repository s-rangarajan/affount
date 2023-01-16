package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
)

func HandleGetTransactionWithContext(ctx context.Context, pool *sql.DB, w http.ResponseWriter, r *http.Request) {
	defer logger.Sync()
	logger.Info("received get transaction request")
	transactionID, err := strconv.ParseUint(r.URL.Query().Get("transaction_id"), 10, 64)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, errors.New("error missing/invalid transaction_id parameter"))
		return
	}
	tenant := r.URL.Query().Get("tenant")
	if tenant == "" {
		writeHTTPError(w, http.StatusBadRequest, errors.New("error missing tenant parameter"))
		return
	}

	logger.Infow("handling get transaction request", "transaction_id", transactionID, "tenant", tenant)
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

	result, err := GetTransactionAndOperationsWithContext(ctx, tx, tenant, transactionID)
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

	marshaledData, err := json.Marshal(result)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
		debug.PrintStack()
		return
	}
	logger.Infow("transaction fetched", "transaction_id", transactionID, "tenant", tenant, "transaction", result)

	w.WriteHeader(http.StatusOK)
	w.Write(marshaledData)
}
