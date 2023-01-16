package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/pressly/goose/v3"
)

type TransactionWithOperations struct {
	Transaction Transaction `json:"transaction"`
	Operations  []Operation `json:"operations"`
}

func CreateAccountWithContext(ctx context.Context, tx *sql.Tx, userARI string) (Account, error) {
	query := `
		INSERT INTO accounts(user_ari)
		VALUES ($1)
		RETURNING
			accounts.account_pk,
			accounts.account_id,
			accounts.user_ari,
			accounts.last_played_sequence,
			accounts.running_balance,
			accounts.running_held
	`

	var account Account
	row := tx.QueryRowContext(ctx, query, userARI)
	if err := row.Scan(
		&account.AccountPK,
		&account.AccountID,
		&account.UserARI,
		&account.LastPlayedSequence,
		&account.RunningBalance,
		&account.RunningHeld,
	); err != nil {
		return Account{}, fmt.Errorf("error executing query: %w", err)
	}

	return account, nil
}

func LockAccountWithContext(ctx context.Context, tx *sql.Tx, accountID uint64) (Account, error) {
	query := `
		SELECT account_pk,
						account_id,
						user_ari,
						last_played_sequence,
						running_balance,
						running_held
		FROM accounts
		WHERE accounts.account_id = $1
		FOR UPDATE
	`

	var account Account
	row := tx.QueryRowContext(ctx, query, accountID)
	if err := row.Scan(
		&account.AccountPK,
		&account.AccountID,
		&account.UserARI,
		&account.LastPlayedSequence,
		&account.RunningBalance,
		&account.RunningHeld,
	); err != nil {
		return Account{}, fmt.Errorf("error executing query: %w", err)
	}

	return account, nil
}

func GetAccountWithContext(ctx context.Context, tx *sql.Tx, accountID uint64) (Account, error) {
	query := `
		SELECT account_pk,
						account_id,
						user_ari,
						last_played_sequence,
						running_balance,
						running_held
		FROM accounts
		WHERE accounts.account_id = $1
	`

	var account Account
	row := tx.QueryRowContext(ctx, query, accountID)
	if err := row.Scan(
		&account.AccountPK,
		&account.AccountID,
		&account.UserARI,
		&account.LastPlayedSequence,
		&account.RunningBalance,
		&account.RunningHeld,
	); err != nil {
		return Account{}, fmt.Errorf("error executing query: %w", err)
	}

	return account, nil
}

func UpdateAccountWithContext(ctx context.Context, tx *sql.Tx, account Account) error {
	query := `
		UPDATE accounts
		SET last_played_sequence = $1,
				running_balance = $2,
				running_held = $3
		WHERE accounts.account_id = $4
	`

	_, err := tx.ExecContext(
		ctx,
		query,
		account.LastPlayedSequence,
		account.RunningBalance,
		account.RunningHeld,
		account.AccountID,
	)

	return err
}

func CreateTransactionAndOperationWithContext(ctx context.Context, tx *sql.Tx, transaction Transaction, operation Operation, event Event) (uint64, error) {
	query := `
		WITH create_transaction AS (
			INSERT INTO transactions(tenant, account_id, held_amount_in_cents, debited_amount_in_cents, credited_amount_in_cents, last_played_sequence)
			VALUES($1, $2, $3, $4, $5, $6)
			RETURNING transactions.transaction_id, transactions.tenant
		), create_operation AS (
			INSERT INTO operations(tenant, transaction_id, operation_type, amount_in_cents, sequence)
			SELECT create_transaction.tenant,
							create_transaction.transaction_id,
							$7,
							$8,
							$9
			FROM create_transaction
			RETURNING operations.tenant,
								operations.transaction_id,
								operations.operation_id
		)
		INSERT INTO events(tenant, account_id, transaction_id, operation_id, sequence, running_balance, running_held)
		SELECT create_operation.tenant,
						$10,
						create_operation.transaction_id,
						create_operation.operation_id,
						$11,
						$12,
						$13
		FROM create_operation
		RETURNING events.transaction_id
	`

	var transactionID uint64
	row := tx.QueryRowContext(
		ctx,
		query,
		transaction.Tenant,
		transaction.AccountID,
		transaction.HeldAmountInCents,
		transaction.DebitedAmountInCents,
		transaction.CreditedAmountInCents,
		transaction.LastPlayedSequence,
		operation.OperationType,
		operation.AmountInCents,
		operation.Sequence,
		transaction.AccountID,
		event.Sequence,
		event.RunningBalance,
		event.RunningHeld,
	)
	if err := row.Scan(&transactionID); err != nil {
		return 0, fmt.Errorf("error executing query: %w", err)
	}

	return transactionID, nil
}

func AddOperationAndUpdateTransactionWithContext(ctx context.Context, tx *sql.Tx, transaction Transaction, operation Operation, event Event) error {
	query := `
		WITH update_transaction AS (
			UPDATE transactions
			SET held_amount_in_cents = $1,
					debited_amount_in_cents = $2,
					credited_amount_in_cents = $3,
					last_played_sequence = $4
			WHERE transactions.tenant = $5
			AND transactions.transaction_id = $6
			RETURNING transactions.transaction_id, transactions.tenant
		), create_operation AS (
			INSERT INTO operations(tenant, transaction_id, operation_type, amount_in_cents, sequence)
			SELECT update_transaction.tenant,
							update_transaction.transaction_id,
							$7,
							$8,
							$9
			FROM update_transaction
			RETURNING operations.tenant,
								operations.transaction_id,
								operations.operation_id
		)
		INSERT INTO events(tenant, account_id, transaction_id, operation_id, sequence, running_balance, running_held)
		SELECT create_operation.tenant,
						$10,
						create_operation.transaction_id,
						create_operation.operation_id,
						$11,
						$12,
						$13
		FROM create_operation
		RETURNING events.account_id,
							events.transaction_id
	`

	_, err := tx.ExecContext(
		ctx,
		query,
		transaction.HeldAmountInCents,
		transaction.DebitedAmountInCents,
		transaction.CreditedAmountInCents,
		transaction.LastPlayedSequence,
		transaction.Tenant,
		transaction.TransactionID,
		operation.OperationType,
		operation.AmountInCents,
		operation.Sequence,
		transaction.AccountID,
		event.Sequence,
		event.RunningBalance,
		event.RunningHeld,
	)

	return err
}

func AddOperationToTransactionWithContext(ctx context.Context, tx *sql.Tx, transaction Transaction, operation Operation, event Event) error {
	query := `
		WITH create_operation AS (
			INSERT INTO operations(tenant, transaction_id, operation_type, amount_in_cents, sequence)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING operations.tenant,
								operations.transaction_id,
								operations.operation_id
		)
		INSERT INTO events(tenant, account_id, transaction_id, operation_id, sequence, running_balance, running_held)
		SELECT create_operation.tenant,
						$6,
						create_operation.transaction_id,
						create_operation.operation_id,
						$7,
						$8,
						$9
		FROM create_operation
		RETURNING events.account_id,
							events.transaction_id
	`

	_, err := tx.ExecContext(
		ctx,
		query,
		transaction.Tenant,
		transaction.TransactionID,
		operation.OperationType,
		operation.AmountInCents,
		operation.Sequence,
		transaction.AccountID,
		event.Sequence,
		event.RunningBalance,
		event.RunningHeld,
	)

	return err
}

func GetTransactionWithContext(ctx context.Context, tx *sql.Tx, tenant string, transactionID uint64) (Transaction, error) {
	query := `
		SELECT transaction_pk,
						transaction_id,
						transactions.tenant,
						account_id,
						held_amount_in_cents,
						debited_amount_in_cents,
						credited_amount_in_cents,
						last_played_sequence
		FROM transactions
		JOIN operations USING(transaction_id, tenant)
		WHERE transactions.tenant = $1
		AND transactions.transaction_id = $2
	`

	var transaction Transaction
	row := tx.QueryRowContext(ctx, query, tenant, transactionID)
	if err := row.Scan(
		&transaction.TransactionPK,
		&transaction.TransactionID,
		&transaction.Tenant,
		&transaction.AccountID,
		&transaction.HeldAmountInCents,
		&transaction.DebitedAmountInCents,
		&transaction.CreditedAmountInCents,
		&transaction.LastPlayedSequence,
	); err != nil {
		return Transaction{}, fmt.Errorf("error executing query: %w", err)
	}

	return transaction, nil
}

func GetTransactionAndOperationsWithContext(ctx context.Context, tx *sql.Tx, tenant string, transactionID uint64) (TransactionWithOperations, error) {
	query := `
		SELECT transaction_pk,
						transaction_id,
						transactions.tenant,
						account_id,
						held_amount_in_cents,
						debited_amount_in_cents,
						credited_amount_in_cents,
						last_played_sequence,
						JSON_AGG(
							JSON_BUILD_OBJECT(
								'operation_pk', operation_pk,
								'operation_id', operation_id,
								'tenant', tenant,
								'transaction_id', transaction_id,
								'operation_type', operation_type,
								'amount_in_cents', amount_in_cents,
								'sequence', sequence
							)
						)
		FROM transactions
		JOIN operations USING(transaction_id, tenant)
		WHERE transactions.tenant = $1
		AND transactions.transaction_id = $2
		GROUP BY transaction_pk
	`

	var transaction Transaction
	var operations []Operation
	var aggregatedData json.RawMessage
	row := tx.QueryRowContext(ctx, query, tenant, transactionID)
	if err := row.Scan(
		&transaction.TransactionPK,
		&transaction.TransactionID,
		&transaction.Tenant,
		&transaction.AccountID,
		&transaction.HeldAmountInCents,
		&transaction.DebitedAmountInCents,
		&transaction.CreditedAmountInCents,
		&transaction.LastPlayedSequence,
		&aggregatedData,
	); err != nil {
		return TransactionWithOperations{}, fmt.Errorf("error executing query: %w", err)
	}
	if err := json.Unmarshal(aggregatedData, &operations); err != nil {
		return TransactionWithOperations{}, fmt.Errorf("error unmarshaling aggregated operations: %w", err)
	}

	return TransactionWithOperations{Transaction: transaction, Operations: operations}, nil
}

func MustSetupDB() (*embeddedpostgres.EmbeddedPostgres, *sql.DB) {
	config := embeddedpostgres.DefaultConfig().Port(5433)
	postgres := embeddedpostgres.NewDatabase(config)
	err := postgres.Start()
	if err != nil {
		logger.Fatal(err)
	}

	pool, err := connect()
	if err != nil {
		logger.Fatal(err)
	}

	if err := goose.Up(pool, "./migrations"); err != nil {
		logger.Fatal(err)
	}

	return postgres, pool
}

func MustSetupRealDB() *sql.DB {
	pool, err := connectReal()
	if err != nil {
		logger.Fatal(err)
	}

	if err := goose.Up(pool, "./migrations"); err != nil {
		logger.Fatal(err)
	}

	return pool
}

func connect() (*sql.DB, error) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@127.0.0.1:5433/postgres?sslmode=disable")
	if err != nil {
		logger.Fatal("error connecting to database: ", err)
	}

	return db, err
}

func connectReal() (*sql.DB, error) {
	db, err := sql.Open("postgres", "postgres://postgres:@127.0.0.1:5432/postgres?sslmode=disable")
	if err != nil {
		logger.Fatal("error connecting to database: ", err)
	}

	return db, err
}
