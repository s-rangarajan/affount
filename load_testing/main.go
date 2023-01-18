package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type createAccountRequest struct {
	UserARI string `json:"user_ari"`
}

type Account struct {
	AccountPK          uint64 `json:"account_pk,omitempty"`
	AccountID          uint64 `json:"account_id"`
	UserARI            string `json:"user_ari"`
	LastPlayedSequence int64  `json:"last_played_sequence"`
	RunningBalance     int64  `json:"running_balance"`
	RunningHeld        int64  `json:"running_held"`
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

const (
	accountContention = 0.3
)

var (
	r             *rand.Rand                     = rand.New(rand.NewSource(time.Now().UnixNano()))
	accounts      map[uint64]map[string][]uint64 = make(map[uint64]map[string][]uint64)
	accountIDs    []uint64                       = make([]uint64, 100)
	numbers                                      = []uint{100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000}
	forwardOps                                   = []string{"RELEASE", "CREDIT"}
	backwardOps                                  = []string{"HOLD", "DEBIT"}
	tenantConfigs                                = []TenantConfig{
		{Tenant: "DPLUS", RandomWalkP: 0.4, NewTransactionBias: 0.8, ReadBias: 0.2, TransactionLengthLimit: 10, Fanout: 10},
		{Tenant: "REFUNDS", RandomWalkP: 0.9, NewTransactionBias: 0.9, ReadBias: 0.1, TransactionLengthLimit: 2, Fanout: 10},
		{Tenant: "PAYNOW", RandomWalkP: 0.5, NewTransactionBias: 0.9, ReadBias: 0.3, TransactionLengthLimit: 10, Fanout: 10},
		{Tenant: "DOUBLOON", RandomWalkP: 0.5, NewTransactionBias: 0.9, ReadBias: 0.4, TransactionLengthLimit: 2, Fanout: 10},
	}
)

func getRandomAccount() uint64 {
	accountContentionBias := 1 - accountContention
	biasedAccountSwath := int(float64(len(accountIDs)) * accountContentionBias)
	return accountIDs[r.Intn(biasedAccountSwath)]
}

// transactions are typically going to be more uniformly distributed
func getRandomTransaction(accountID uint64, tenant string) uint64 {
	transactions := accounts[accountID][tenant]
	return transactions[r.Intn(len(transactions))]
}

func main() {
	log.SetFlags(0)
	log.Println("init load tests")

	errChan := make(chan struct{}, 10000000)
	httpReadAccountErrorChan := make(chan struct{}, 10000000)
	httpReadTransactionErrorChan := make(chan struct{}, 10000000)
	httpExecuteOperationsErrorChan := make(chan struct{}, 10000000)
	opSuccessChan := make(chan struct{}, 10000000)
	txnSuccessChan := make(chan struct{}, 10000000)
	readSuccessChan := make(chan struct{}, 10000000)
	go func() {
		var errCount, httpReadAccountErrorCount, httpReadTransactionErrorCount, httpExecuteOperationsErrorCount, opSuccessCount, txnSuccessCount, readSuccessCount uint
		go func() {
			ticker := time.NewTicker(1000 * time.Millisecond)
			log.Printf("errs,ReadAcctErrors,ReadTxnErrors,ExecOpsErrors,OpSuccesses,TxnSuccesses,ReadSuccesses")
			for {
				select {
				case <-ticker.C:
					log.Printf("%d,%d,%d,%d,%d,%d,%d", errCount, httpReadAccountErrorCount, httpReadTransactionErrorCount, httpExecuteOperationsErrorCount, opSuccessCount, txnSuccessCount, readSuccessCount)
				}
			}
		}()
		for {
			select {
			case <-errChan:
				errCount++
			case <-httpReadAccountErrorChan:
				httpReadAccountErrorCount++
			case <-httpReadTransactionErrorChan:
				httpReadTransactionErrorCount++
			case <-httpExecuteOperationsErrorChan:
				httpExecuteOperationsErrorCount++
			case <-opSuccessChan:
				opSuccessCount++
			case <-txnSuccessChan:
				txnSuccessCount++
			case <-readSuccessChan:
				readSuccessCount++
			}
		}
	}()

	log.Println("setup metric collection")

	log.Println("setting up accounts and transactions")
	for i := 0; i < len(accountIDs); i++ {
		log.Printf("processing account %d", i)
		account, statusCode, err := CreateAccount(uuid.New().String())
		if err != nil {
			log.Fatalf("error setting up accounts: %s", err.Error())
		}
		if statusCode != 200 {
			log.Fatalf("error setting up accounts, http statuscode: %d", statusCode)
		}
		accountIDs[i] = account.AccountID
		accounts[account.AccountID] = make(map[string][]uint64)
		for j := range tenantConfigs {
			accounts[account.AccountID][tenantConfigs[j].Tenant] = make([]uint64, 10)
			for k := 0; k < len(accounts[account.AccountID][tenantConfigs[j].Tenant]); k++ {
				result, statusCode, err := CreateTransaction(account.AccountID, tenantConfigs[j].Tenant)
				if err != nil {
					log.Fatalf("error setting up transactions: %s", err.Error())
				}
				if statusCode != 200 {
					log.Fatalf("error setting up transactions, http statuscode: %d", statusCode)
				}
				accounts[account.AccountID][tenantConfigs[j].Tenant][k] = result.Transaction.TransactionID
			}
		}
	}
	log.Println("set up accounts and transactions")

	log.Println("starting load test")
	var wg sync.WaitGroup
	for i := range tenantConfigs {
		tester := NewTenantTester(tenantConfigs[i], errChan, httpReadAccountErrorChan, httpReadTransactionErrorChan, httpExecuteOperationsErrorChan, opSuccessChan, txnSuccessChan, readSuccessChan)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tester.Spawn()
		}()
	}

	wg.Wait()
	fmt.Println("load tests done")
}

func CreateAccount(userARI string) (Account, int, error) {
	request := createAccountRequest{UserARI: userARI}
	requestBody, _ := json.Marshal(request)
	response, err := http.Post("http://localhost:8080/create_account", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return Account{}, 0, fmt.Errorf("error posting create account request: %w", err)
	}
	defer response.Body.Close()

	var account Account
	if err := json.NewDecoder(response.Body).Decode(&account); err != nil {
		return Account{}, 0, fmt.Errorf("error unmarshaling create account response: %w", err)
	}

	return account, response.StatusCode, nil
}

func CreateTransaction(accountID uint64, tenant string) (executeOperationsResponse, int, error) {
	request := executeOperationsRequest{
		AccountID:  accountID,
		Tenant:     tenant,
		Operations: []operationRequest{{OperationType: "CREDIT", AmountInCents: 10000}},
	}
	requestBody, _ := json.Marshal(request)

	return ExecuteOperations(requestBody)
}

func ExecuteOperations(requestBody json.RawMessage) (executeOperationsResponse, int, error) {
	response, err := http.Post("http://localhost:8080/execute_operations", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return executeOperationsResponse{}, 0, fmt.Errorf("error posting execute operations request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return executeOperationsResponse{}, response.StatusCode, fmt.Errorf("error execute operations returned non 200: %d", response.StatusCode)
	}

	var operationsResponse executeOperationsResponse
	if err := json.NewDecoder(response.Body).Decode(&operationsResponse); err != nil {
		return executeOperationsResponse{}, 0, fmt.Errorf("error unmarshaling execute operations response: %w", err)
	}

	return operationsResponse, response.StatusCode, nil
}

func ReadAccount(accountID uint64) (Account, int, error) {
	response, err := http.Get(fmt.Sprintf("http://localhost:8080/get_account?account_id=%d", accountID))
	if err != nil {
		return Account{}, 0, fmt.Errorf("error executing get account request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return Account{}, response.StatusCode, fmt.Errorf("error received non 200 getting account: %d", response.StatusCode)
	}

	var account Account
	if err := json.NewDecoder(response.Body).Decode(&account); err != nil {
		return Account{}, response.StatusCode, fmt.Errorf("error unmarshaling get account response: %w", err)
	}

	return account, response.StatusCode, nil
}

func ReadTransaction(tenant string, transactionID uint64) (Transaction, int, error) {
	response, err := http.Get(fmt.Sprintf("http://localhost:8080/get_transaction?tenant=%s&transaction_id=%d", tenant, transactionID))
	if err != nil {
		return Transaction{}, 0, fmt.Errorf("error executing get transaction request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return Transaction{}, response.StatusCode, fmt.Errorf("error received non 200 getting transaction: %d", response.StatusCode)
	}

	var transaction Transaction
	if err := json.NewDecoder(response.Body).Decode(&transaction); err != nil {
		return Transaction{}, 0, fmt.Errorf("error unmarshaling get transaction response: %w", err)
	}

	return transaction, response.StatusCode, nil
}
