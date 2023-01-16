package main

import (
	"errors"
	"fmt"
)

type TxOp int64

const (
	Hold TxOp = iota
	Release
	Debit
	Credit
)

var ErrInvalidPlayOrderNegativeBalance = errors.New("invalid order of operations, results in negative account balance")
var ErrInvalidPlayOrderNegativeHold = errors.New("invalid order of operations, results in negatively held amount")
var ErrAccountOperationLimit = errors.New("account limit on operations reached")
var ErrTransactionOperationLimit = errors.New("transaction limit on operations reached")

// most sql drivers and go's native driver definitely
// do not support setting the high bit, so realistically,
// even if we have uint64s, we're only getting 50% of that
// in the Go data structure. but, it does prevent assignment
// and unmarshaling of -ve values and that's worth something.
// however, the arithmetic fields are all int64 to make it
// simpler to detect overflow because it is unwarranted
// complexity to deal with the modular arithmetic wraparound
// considering that the values can and should never be negative

type Account struct {
	AccountPK          uint64 `json:"account_pk,omitempty"`
	AccountID          uint64 `json:"account_id"`
	UserARI            string `json:"user_ari"`
	LastPlayedSequence int64  `json:"last_played_sequence"`
	RunningBalance     int64  `json:"running_balance"`
	RunningHeld        int64  `json:"running_held"`
}

type PlayedOutcome struct {
	PlayedAccount     Account
	PlayedTransaction Transaction
	PlayedOperations  []Operation
	PlayedEvents      []Event
}

// the concept of atomically  playing multiple operations in a single
// API call only extends to a single transaction. this is intentional.
// while it might be cute to extend this across transaction boundaries,
// realistically, it makes little sense for related operations to be
// spread out across multiple transactions.
func (account Account) Play(transaction Transaction, operations []Operation) (PlayedOutcome, error) {
	// primitives only, copied by value
	playedTransaction := transaction
	playedAccount := account
	playedOperations := make([]Operation, len(operations))
	playedEvents := make([]Event, len(playedOperations))

	logger.Infow("playing operations", "account", account, "transaction", transaction, "operations", operations)

	for i := range operations {
		// primitives only, copied by value
		playedOperation := operations[i]
		logger.Infow("playing operation", "account", playedAccount, "transaction", playedTransaction, "operation", playedOperation)

		operationType, err := playedOperation.Type()
		if err != nil {
			return PlayedOutcome{}, fmt.Errorf("error getting operation type: %w", err)
		}
		switch operationType {
		case Hold:
			playedTransaction.HeldAmountInCents += playedOperation.AmountInCents
			playedAccount.RunningHeld += playedOperation.AmountInCents
		case Release:
			playedTransaction.HeldAmountInCents -= playedOperation.AmountInCents
			playedAccount.RunningHeld -= playedOperation.AmountInCents
		case Debit:
			playedTransaction.DebitedAmountInCents += playedOperation.AmountInCents
			playedAccount.RunningBalance -= playedOperation.AmountInCents
		case Credit:
			playedTransaction.CreditedAmountInCents += playedOperation.AmountInCents
			playedAccount.RunningBalance += playedOperation.AmountInCents
		default:
			continue
		}

		if playedAccount.RunningBalance < 0 {
			return PlayedOutcome{}, ErrInvalidPlayOrderNegativeBalance
		}
		if playedAccount.RunningHeld < 0 {
			if playedTransaction.HeldAmountInCents >= 0 {
				logger.Fatalf("accounting inconsistency, triage needed")
			}
		}
		if playedTransaction.HeldAmountInCents < 0 {
			return PlayedOutcome{}, ErrInvalidPlayOrderNegativeHold
		}
		// signed wraparound
		if playedAccount.LastPlayedSequence < 0 {
			return PlayedOutcome{}, ErrAccountOperationLimit
		}
		// signed wraparound
		if playedTransaction.LastPlayedSequence < 0 {
			return PlayedOutcome{}, ErrTransactionOperationLimit
		}

		playedAccount.LastPlayedSequence += 1
		playedTransaction.LastPlayedSequence += 1
		playedOperation.Sequence = playedTransaction.LastPlayedSequence
		playedOperations[i] = playedOperation
		logger.Infow("played operation", "account", playedAccount, "transaction", playedTransaction, "operation", playedOperation)
		event := Event{
			AccountID:      account.AccountID,
			Sequence:       playedAccount.LastPlayedSequence,
			RunningBalance: playedAccount.RunningBalance,
			RunningHeld:    playedAccount.RunningHeld,
		}
		playedEvents[i] = event
	}

	return PlayedOutcome{
		PlayedAccount:     playedAccount,
		PlayedTransaction: playedTransaction,
		PlayedOperations:  playedOperations,
		PlayedEvents:      playedEvents,
	}, nil
}

type Transaction struct {
	TransactionPK         uint64 `json:"transaction_pk,omitempty"`
	TransactionID         uint64 `json:"transaction_id"`
	Tenant                string `json:"tenant"`
	AccountID             uint64 `json:"account_id"`
	HeldAmountInCents     int64  `json:"held_amount_in_cents"`
	DebitedAmountInCents  int64  `json:"debited_amount_in_cents"`
	CreditedAmountInCents int64  `json:"credited_amount_in_cents"`
	LastPlayedSequence    int64  `json:"last_played_sequence"`
}

type Operation struct {
	OperationPK   uint64 `json:"operation_pk"`
	OperationID   uint64 `json:"operation_id"`
	Tenant        string `json:"tenant"`
	TransactionID uint64 `json:"transaction_id"`
	OperationType string `json:"operation_type"`
	AmountInCents int64  `json:"amount_in_cents"`
	Sequence      int64  `json:"sequence"`
}

func (o Operation) Type() (TxOp, error) {
	switch o.OperationType {
	case "HOLD":
		return Hold, nil
	case "RELEASE":
		return Release, nil
	case "DEBIT":
		return Debit, nil
	case "CREDIT":
		return Credit, nil
	default:
		return 0, fmt.Errorf("unknown operation type")
	}
}

type Event struct {
	EventPK        uint64 `json:"event_pk"`
	EventID        uint64 `json:"event_id"`
	Tenant         string `json:"tenant"`
	AccountID      uint64 `json:"account_id"`
	TransactionID  uint64 `json:"transaction_id"`
	OperationID    uint64 `json:"operation_id"`
	RunningBalance int64  `json:"running_balance"`
	RunningHeld    int64  `json:"running_held"`
	Sequence       int64  `json:"sequence"`
}
