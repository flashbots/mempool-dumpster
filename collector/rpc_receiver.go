package collector

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/flashbots/mempool-dumpster/metrics"
)

const (
	rpcAuthLifetime            = time.Hour
	rpcAuthRefreshBeforeExpiry = time.Minute
	rpcRequestID               = 1
	maxRPCResponseSize         = 8 << 10
)

var (
	errRPCReceiverURL     = errors.New("expected an HTTPS URL without user info")
	errRPCSigningKeyFile  = errors.New("JSON-RPC receivers require a signing key file")
	errRPCSigningKey      = errors.New("invalid receiver signing key")
	errRPCRequest         = errors.New("create JSON-RPC receiver request")
	errRPCSend            = errors.New("send JSON-RPC receiver request")
	errRPCStatus          = errors.New("JSON-RPC receiver returned HTTP status")
	errRPCReadResponse    = errors.New("read JSON-RPC receiver response")
	errRPCInvalidResponse = errors.New("invalid JSON-RPC receiver response")
	errRPCResponseCode    = errors.New("JSON-RPC receiver returned error code")
)

type rpcCredential struct {
	auth      string
	signature string
	expiresAt time.Time
}

type rpcAuth struct {
	key    *ecdsa.PrivateKey
	signer string

	mu         sync.Mutex
	credential rpcCredential
}

func (a *rpcAuth) headers(now time.Time) (rpcCredential, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if now.Before(a.credential.expiresAt.Add(-rpcAuthRefreshBeforeExpiry)) {
		return a.credential, nil
	}

	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return rpcCredential{}, fmt.Errorf("generate receiver authentication token: %w", err)
	}
	expiresAt := now.Add(rpcAuthLifetime).Truncate(time.Second)
	auth := fmt.Sprintf("flashbots-auth-v2:%d:%s", expiresAt.Unix(), hex.EncodeToString(token[:]))
	signature, err := crypto.Sign(accounts.TextHash([]byte(auth)), a.key)
	if err != nil {
		return rpcCredential{}, fmt.Errorf("sign receiver authentication token: %w", err)
	}
	a.credential = rpcCredential{
		auth:      auth,
		signature: a.signer + ":" + hexutil.Encode(signature),
		expiresAt: expiresAt,
	}
	return a.credential, nil
}

type rpcReceiver struct {
	url    string
	client *http.Client
	auth   *rpcAuth
}

// NewRPCReceivers creates HTTPS JSON-RPC receivers that share one signer.
func NewRPCReceivers(urls []string, keyFile string) ([]TxReceiver, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	for i, endpoint := range urls {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil {
			return nil, fmt.Errorf("JSON-RPC receiver %d: %w", i+1, errRPCReceiverURL)
		}
	}
	if keyFile == "" {
		return nil, errRPCSigningKeyFile
	}
	keyBytes, err := os.ReadFile(filepath.Clean(keyFile))
	if err != nil {
		return nil, fmt.Errorf("read receiver signing key: %w", err)
	}
	keyHex := strings.TrimPrefix(strings.TrimSpace(string(keyBytes)), "0x")
	key, err := crypto.HexToECDSA(keyHex)
	if err != nil {
		return nil, errRPCSigningKey
	}
	auth := &rpcAuth{key: key, signer: crypto.PubkeyToAddress(key.PublicKey).Hex()}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	receivers := make([]TxReceiver, 0, len(urls))
	for _, endpoint := range urls {
		receivers = append(receivers, &rpcReceiver{url: endpoint, client: client, auth: auth})
	}
	return receivers, nil
}

type rpcRequest struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Method  string    `json:"method"`
	Params  [1]string `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  *ethcommon.Hash `json:"result"`
	Error   *struct {
		Code int `json:"code"`
	} `json:"error"`
}

func (r *rpcReceiver) SendTx(ctx context.Context, tx *common.TxIn) error {
	raw, err := tx.Tx.MarshalBinary()
	if err != nil {
		return fmt.Errorf("encode receiver transaction: %w", err)
	}
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      rpcRequestID,
		Method:  "eth_sendRawTransaction",
		Params:  [1]string{hexutil.Encode(raw)},
	})
	if err != nil {
		return fmt.Errorf("encode JSON-RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return errRPCRequest
	}
	headers, err := r.auth.headers(time.Now())
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Flashbots-Auth", headers.auth)
	req.Header.Set("X-Flashbots-Signature", headers.signature)

	outcome := "transport_error"
	defer func() { metrics.IncTxRPCReceiverOutcome(outcome) }()
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("send JSON-RPC receiver request: %w", ctx.Err())
		}
		return errRPCSend
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		outcome = "http_error"
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxRPCResponseSize))
		return fmt.Errorf("%w %d", errRPCStatus, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxRPCResponseSize+1))
	if err != nil {
		return errRPCReadResponse
	}
	outcome = "invalid_response"
	var response rpcResponse
	if len(data) > maxRPCResponseSize || json.Unmarshal(data, &response) != nil || response.JSONRPC != "2.0" || response.ID != rpcRequestID {
		return errRPCInvalidResponse
	}
	if response.Error != nil {
		outcome = "rpc_error"
		return fmt.Errorf("%w %d", errRPCResponseCode, response.Error.Code)
	}
	if response.Result == nil || *response.Result != tx.Tx.Hash() {
		return fmt.Errorf("%w: unexpected transaction hash", errRPCInvalidResponse)
	}
	outcome = "success"
	return nil
}
