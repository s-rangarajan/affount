package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	uuid "github.com/satori/go.uuid"
)

const holdTimeout = shutdownGracePeriod - 2*time.Second

type HoldRequest struct {
	Tenant             string `json:"tenant"`
	UserARI            string `json:"user_ari"`
	AmountInCents      uint   `json:"amount_in_cents"`
	ClientIdentifier   string `json:"client_identifier"`
	ClientUUID         string `json:"client_uuid"`
	HoldDurationInDays uint   `json:"hold_duration"`
}

func (h *HoldRequest) Validate() error {
	if h.Tenant == "" {
		return fmt.Errorf("missing tenant")
	}

	if h.UserARI == "" {
		return fmt.Errorf("missing user_ari")
	}

	if h.AmountInCents == "" {
		return fmt.Errorf("missing amount_in_cents")
	}

	if h.ClientIdentifer == "" {
		return fmt.Errorf("missing client_identifier")
	}

	if h.ClientUUID == "" {
		return fmt.Errorf("missing client_uuid")
	}

	if h.HoldDurationInDays > 31 {
		return fmt.Errorf("hold_duration cannot be > 31 days")
	}
}

type HoldResponse struct {
	TransactionUUID string `json:"transaction_uuid"`
	IntentUUID      string `json:"intent_uuid"`
	Status          string `json:"status"`
}

func HoldWithContext(ctx context.Context, Banker banker, w http.ResponseWriter, r *http.Request) {
	transactionUUID := uuid.NewV4().String()
	intentUUID := uuid.NewV4().String()

	computeStart := time.Now()
	ctx, cancelFunc := context.WithTimeout(ctx, computeTimeout)
	defer cancelFunc()

	w.Header().Set("Content-Type", "application/json")
	var holdRequest HoldRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&holdRequest); err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{fmt.Errorf("error unmarshaling request: %w", err).Error()})
		return
	}

	if err := holdRequest.Validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{fmt.Errorf("invalid request: %w", err).Error()})
		return
	}

	holdResult, err := banker.ExecuteHoldWithContext(ctx, holdRequest)
	if err != nil {
		if err == context.DeadlineExceeded {
			json.NewEncoder(w).Encode(HoldResponse{})
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}

	}
}
