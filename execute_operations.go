package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
)

type operationRequest struct {
	OperationType string `json:"operation_type"`
	AmountInCents int64  `json:"amount_in_cents"`
}

type executeOperationsRequest struct {
	AccountID     uint64             `json:"account_id"`
	Tenant        string             `json:"tenant"`
	TransactionID uint64             `json:"transaction_id"`
	Operations    []operationRequest `json:"operations"`
}

type executeOperationsResponse struct {
	Error       string      `json:"error"`
	Account     Account     `json:"account,omitempty"`
	Transaction Transaction `json:"transaction,omitempty"`
}

func HandleExecuteOperationsWithContext(ctx context.Context, pool *sql.DB, w http.ResponseWriter, r *http.Request) {
	defer logger.Sync()
	logger.Info("received execute operations request")
	if r.Body == nil {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error empty request body"))
		return
	}

	var req executeOperationsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, http.StatusUnprocessableEntity, fmt.Errorf("error decoding request body: %w", err))
		return
	}

	if req.Tenant == "" {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error missing required fields"))
		return
	}
	if len(req.Operations) == 0 {
		writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error missing required fields"))
		return
	}
	for i := range req.Operations {
		if req.Operations[i].OperationType == "" || req.Operations[i].AmountInCents <= 0 {
			writeHTTPError(w, http.StatusBadRequest, fmt.Errorf("error missing/invalid required fields"))
			return
		}
	}

	logger.Infow("handling execute operations request", "request", req)
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

	account, err := LockAccountWithContext(ctx, tx, req.AccountID)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error executing database operations: %w", err))
		debug.PrintStack()
		return
	}

	var result executeOperationsResponse
	if req.TransactionID != 0 {
		transaction, err := GetTransactionWithContext(ctx, tx, req.Tenant, req.TransactionID)
		if err != nil {
			writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error retrieving transaction data: %w", err))
			debug.PrintStack()
			return
		}

		result, err = processExistingTransaction(ctx, tx, req, account, transaction)
		if errors.Is(err, ErrInvalidPlayOrderNegativeBalance) || errors.Is(err, ErrInvalidPlayOrderNegativeHold) {
			errorResult := executeOperationsResponse{
				Error:       err.Error(),
				Account:     account,
				Transaction: transaction,
			}

			marshaledData, err := json.Marshal(errorResult)
			if err != nil {
				writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
				debug.PrintStack()
				return
			}
			w.WriteHeader(http.StatusUnprocessableEntity)
			w.Write(marshaledData)
			return
		}
	} else {
		result, err = processNewTransaction(ctx, tx, req, account)
		if errors.Is(err, ErrInvalidPlayOrderNegativeBalance) || errors.Is(err, ErrInvalidPlayOrderNegativeHold) {
			errorResult := executeOperationsResponse{
				Error:   err.Error(),
				Account: account,
			}

			marshaledData, err := json.Marshal(errorResult)
			if err != nil {
				writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
				debug.PrintStack()
				return
			}
			w.WriteHeader(http.StatusUnprocessableEntity)
			w.Write(marshaledData)
			return
		}
	}
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error processing operations: %w", err))
		debug.PrintStack()
		return
	}

	if err := tx.Commit(); err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error committing database state: %w", err))
		debug.PrintStack()
		return
	}
	logger.Infow("operations executed", "request", req, "result", result)

	marshaledData, err := json.Marshal(result)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("error marshaling response: %w", err))
		debug.PrintStack()
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(marshaledData)
}

func processNewTransaction(ctx context.Context, tx *sql.Tx, req executeOperationsRequest, account Account) (executeOperationsResponse, error) {
	transaction := Transaction{AccountID: req.AccountID, Tenant: req.Tenant}
	operations := make([]Operation, len(req.Operations))
	for i := range req.Operations {
		operations[i] = Operation{OperationType: req.Operations[i].OperationType, AmountInCents: req.Operations[i].AmountInCents}
	}

	playedOutcome, err := account.Play(transaction, operations)
	if err != nil {
		return executeOperationsResponse{}, fmt.Errorf("error playing operations: %w", err)
	}

	for i := range playedOutcome.PlayedOperations {
		if i == 0 {
			transactionID, err := CreateTransactionAndOperationWithContext(ctx, tx, playedOutcome.PlayedTransaction, playedOutcome.PlayedOperations[i], playedOutcome.PlayedEvents[i])
			if err != nil {
				return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
			}
			playedOutcome.PlayedTransaction.TransactionID = transactionID
			continue
		}

		if i == len(playedOutcome.PlayedOperations)-1 {
			if err := AddOperationAndUpdateTransactionWithContext(ctx, tx, playedOutcome.PlayedTransaction, playedOutcome.PlayedOperations[i], playedOutcome.PlayedEvents[i]); err != nil {
				return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
			}
			break
		}

		if err := AddOperationToTransactionWithContext(ctx, tx, playedOutcome.PlayedTransaction, playedOutcome.PlayedOperations[i], playedOutcome.PlayedEvents[i]); err != nil {
			return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
		}
	}

	if err := UpdateAccountWithContext(ctx, tx, playedOutcome.PlayedAccount); err != nil {
		return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
	}

	return executeOperationsResponse{Account: playedOutcome.PlayedAccount, Transaction: playedOutcome.PlayedTransaction}, nil
}

func processExistingTransaction(ctx context.Context, tx *sql.Tx, req executeOperationsRequest, account Account, transaction Transaction) (executeOperationsResponse, error) {
	operations := make([]Operation, len(req.Operations))
	for i := range req.Operations {
		operations[i] = Operation{OperationType: req.Operations[i].OperationType, AmountInCents: req.Operations[i].AmountInCents}
	}

	playedOutcome, err := account.Play(transaction, operations)
	if err != nil {
		return executeOperationsResponse{}, fmt.Errorf("error playing operations: %w", err)
	}

	for i := range playedOutcome.PlayedOperations {
		if i == len(playedOutcome.PlayedOperations)-1 {
			if err := AddOperationAndUpdateTransactionWithContext(ctx, tx, playedOutcome.PlayedTransaction, playedOutcome.PlayedOperations[i], playedOutcome.PlayedEvents[i]); err != nil {
				return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
			}
			break
		}

		if err := AddOperationToTransactionWithContext(ctx, tx, playedOutcome.PlayedTransaction, playedOutcome.PlayedOperations[i], playedOutcome.PlayedEvents[i]); err != nil {
			return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
		}
	}

	if err := UpdateAccountWithContext(ctx, tx, playedOutcome.PlayedAccount); err != nil {
		return executeOperationsResponse{}, fmt.Errorf("error updating played outcome state: %w", err)
	}

	return executeOperationsResponse{Account: playedOutcome.PlayedAccount, Transaction: playedOutcome.PlayedTransaction}, nil
}
