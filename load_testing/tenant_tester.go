package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"
)

type TenantConfig struct {
	Tenant string
	// P = forward, forward = credit
	// P < 0.5, certain multiple deaths
	RandomWalkP            float64
	NewTransactionBias     float64
	ReadBias               float64
	TransactionLengthLimit uint
	Fanout                 uint
}

type TenantTester struct {
	rand                           *rand.Rand
	errChan                        chan<- struct{}
	httpReadAccountErrorChan       chan<- struct{}
	httpReadTransactionErrorChan   chan<- struct{}
	httpExecuteOperationsErrorChan chan<- struct{}
	opSuccessChan                  chan<- struct{}
	txnSuccessChan                 chan<- struct{}
	readSuccessChan                chan<- struct{}

	TenantConfig
}

func NewTenantTester(
	tenantConfig TenantConfig,
	errChan chan<- struct{},
	httpReadAccountErrorChan chan<- struct{},
	httpReadTransactionErrorChan chan<- struct{},
	httpExecuteOperationsErrorChan chan<- struct{},
	opSuccessChan chan<- struct{},
	txnSuccessChan chan<- struct{},
	readSuccessChan chan<- struct{},
) TenantTester {
	return TenantTester{
		rand:                           rand.New(rand.NewSource(time.Now().UnixNano())),
		errChan:                        errChan,
		httpReadAccountErrorChan:       httpReadAccountErrorChan,
		httpReadTransactionErrorChan:   httpReadTransactionErrorChan,
		httpExecuteOperationsErrorChan: httpExecuteOperationsErrorChan,
		opSuccessChan:                  opSuccessChan,
		txnSuccessChan:                 txnSuccessChan,
		readSuccessChan:                readSuccessChan,
		TenantConfig:                   tenantConfig,
	}
}

func (t TenantTester) AssembleRandomNewTransaction(accountID uint64, opLen uint) json.RawMessage {
	req := executeOperationsRequest{
		AccountID: accountID,
		Tenant:    t.Tenant,
	}

	// otherwise no data
	if opLen < 1 {
		opLen = 1
	}

	for i := 0; i < int(opLen); i++ {
		var op string
		if t.rand.Float64() < t.RandomWalkP {
			op = backwardOps[t.rand.Intn(len(backwardOps))]
		} else {
			op = forwardOps[t.rand.Intn(len(forwardOps))]
		}
		opReq := operationRequest{
			OperationType: op,
			AmountInCents: int64(numbers[t.rand.Intn(len(numbers))]),
		}
		req.Operations = append(req.Operations, opReq)
	}

	m, _ := json.Marshal(req)
	return m
}

func (t TenantTester) AssembleRandomOperations(accountID uint64, transactionID uint64, opLen uint) json.RawMessage {
	req := executeOperationsRequest{
		TransactionID: transactionID,
		AccountID:     accountID,
		Tenant:        t.Tenant,
	}

	// otherwise no data
	if opLen < 1 {
		opLen = 1
	}

	for i := 0; i < int(opLen); i++ {
		var op string
		if t.rand.Float64() < t.RandomWalkP {
			op = backwardOps[t.rand.Intn(len(backwardOps))]
		} else {
			op = forwardOps[t.rand.Intn(len(forwardOps))]
		}
		opReq := operationRequest{
			OperationType: op,
			AmountInCents: int64(numbers[t.rand.Intn(len(numbers))]),
		}
		req.Operations = append(req.Operations, opReq)
	}

	m, _ := json.Marshal(req)
	return m
}

func (t TenantTester) RunRandomNewTransactionScenario() {
	accountID := getRandomAccount()
	opLen := uint(t.rand.Intn(int(t.TransactionLengthLimit)))
	requestBody := t.AssembleRandomNewTransaction(accountID, opLen)
	response, statusCode, err := ExecuteOperations(requestBody)
	if statusCode > 200 {
		// log.Println("execute operations statuscode", statusCode)
		t.httpExecuteOperationsErrorChan <- struct{}{}
		return
	}
	if err != nil {
		log.Println("execute operations error", err.Error())
		t.errChan <- struct{}{}
		return
	}
	t.txnSuccessChan <- struct{}{}
	for i := 0; i < int(opLen); i++ {
		t.opSuccessChan <- struct{}{}
	}

	transactionID := response.Transaction.TransactionID
	for {
		if t.rand.Float64() < t.ReadBias {
			_, statusCode, err = ReadAccount(accountID)
			if statusCode > 200 {
				log.Println("read account statuscode", statusCode)
				t.httpReadAccountErrorChan <- struct{}{}
				return
			}
			if err != nil {
				log.Println("read account error", err.Error())
				t.errChan <- struct{}{}
				return
			}
			t.readSuccessChan <- struct{}{}

			_, statusCode, err = ReadTransaction(t.Tenant, transactionID)
			if statusCode > 200 {
				log.Println("read transaction statuscode", statusCode)
				t.httpReadTransactionErrorChan <- struct{}{}
				return
			}
			if err != nil {
				log.Println("read transaction error", err.Error())
				t.errChan <- struct{}{}
				return
			}
			t.readSuccessChan <- struct{}{}
		}
		requestBody := t.AssembleRandomOperations(accountID, transactionID, 1)
		_, statusCode, err = ExecuteOperations(requestBody)
		if statusCode > 200 {
			// log.Println("execute operations statuscode", statusCode)
			t.httpExecuteOperationsErrorChan <- struct{}{}
			continue
		}
		if err != nil {
			log.Println("execute operations error", err.Error())
			t.errChan <- struct{}{}
			return
		}
		t.txnSuccessChan <- struct{}{}
		t.opSuccessChan <- struct{}{}
		if t.rand.Float64() < t.NewTransactionBias {
			return
		}
	}
}

func (t TenantTester) RunExtendExistingTransasctionScenario() {
	accountID := getRandomAccount()
	transactionID := getRandomTransaction(accountID, t.Tenant)
	opLen := uint(t.rand.Intn(int(t.TransactionLengthLimit)))
	requestBody := t.AssembleRandomOperations(accountID, transactionID, opLen)
	_, statusCode, err := ExecuteOperations(requestBody)
	if statusCode > 200 {
		// log.Println("execute operations statuscode", statusCode)
		t.httpExecuteOperationsErrorChan <- struct{}{}
		return
	}
	if err != nil {
		log.Println("execute operations error", err.Error())
		t.errChan <- struct{}{}
		return
	}
	t.txnSuccessChan <- struct{}{}
	for i := 0; i < int(opLen); i++ {
		t.opSuccessChan <- struct{}{}
	}

	for {
		if t.rand.Float64() < t.ReadBias {
			_, statusCode, err = ReadAccount(accountID)
			if statusCode > 200 {
				log.Println("read account statuscode", statusCode)
				t.httpReadAccountErrorChan <- struct{}{}
				return
			}
			if err != nil {
				log.Println("read account error", err.Error())
				t.errChan <- struct{}{}
				return
			}
			t.readSuccessChan <- struct{}{}

			_, statusCode, err = ReadTransaction(t.Tenant, transactionID)
			if statusCode > 200 {
				log.Println("read transaction statuscode", statusCode)
				t.httpReadTransactionErrorChan <- struct{}{}
				return
			}
			if err != nil {
				log.Println("read transaction error", err.Error())
				t.errChan <- struct{}{}
				return
			}
			t.readSuccessChan <- struct{}{}
		}
		requestBody := t.AssembleRandomOperations(accountID, transactionID, 1)
		_, statusCode, err := ExecuteOperations(requestBody)
		if statusCode > 200 {
			// log.Println("execute operations statuscode", statusCode)
			t.httpExecuteOperationsErrorChan <- struct{}{}
			continue
		}
		if err != nil {
			log.Println("execute operations error", err.Error())
			t.errChan <- struct{}{}
			return
		}
		t.txnSuccessChan <- struct{}{}
		t.opSuccessChan <- struct{}{}
		if t.rand.Float64() < t.NewTransactionBias {
			return
		}
	}
}

func (t TenantTester) Work() {
	for {
		if t.rand.Float64() < t.NewTransactionBias {
			t.RunRandomNewTransactionScenario()
			continue
		}
		t.RunExtendExistingTransasctionScenario()
	}
}

func (t TenantTester) Spawn() {
	var wg sync.WaitGroup
	for i := 0; i < int(t.Fanout); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Work()
		}()
	}

	wg.Wait()
}
