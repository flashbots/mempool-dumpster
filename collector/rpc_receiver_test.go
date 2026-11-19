package collector

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCReceiverSendsAuthenticatedTransaction(t *testing.T) {
	key, keyFile := rpcReceiverTestKey(t)
	firstTx := rpcReceiverTestTransaction(t, key, 0)
	secondTx := rpcReceiverTestTransaction(t, key, 1)
	type receivedRequest struct {
		headers http.Header
		body    []byte
	}
	requests := make(chan receivedRequest, 2)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		requests <- receivedRequest{headers: r.Header, body: body}
		hash := firstTx.Hash()
		if r.URL.Path == "/second" {
			hash = secondTx.Hash()
		}
		_, err = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%q}`, hash.Hex())
		assert.NoError(t, err)
	}))
	defer server.Close()

	receivers, err := NewRPCReceivers([]string{server.URL, server.URL + "/second"}, keyFile)
	require.NoError(t, err)
	var firstAuth, firstSignature string
	for i, tx := range []*types.Transaction{firstTx, secondTx} {
		receiver := rpcReceiverWithTestTransport(t, receivers[i], server)
		require.NoError(t, receiver.SendTx(t.Context(), &common.TxIn{Tx: tx}))

		request := <-requests
		require.Equal(t, "application/json", request.headers.Get("Content-Type"))
		raw, err := tx.MarshalBinary()
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[%q]}`, hexutil.Encode(raw)), string(request.body))

		auth := request.headers.Get("X-Flashbots-Auth")
		signature := request.headers.Get("X-Flashbots-Signature")
		if i == 0 {
			firstAuth, firstSignature = auth, signature
		}
		require.Equal(t, []string{firstAuth}, request.headers.Values("X-Flashbots-Auth"))
		require.Equal(t, []string{firstSignature}, request.headers.Values("X-Flashbots-Signature"))
		parts := strings.Split(auth, ":")
		require.Len(t, parts, 3)
		require.Equal(t, "flashbots-auth-v2", parts[0])
		expiry, err := strconv.ParseInt(parts[1], 10, 64)
		require.NoError(t, err)
		now := time.Now()
		require.True(t, time.Unix(expiry, 0).After(now))
		require.False(t, time.Unix(expiry, 0).After(now.Add(24*time.Hour)))
		require.Regexp(t, `^[a-zA-Z0-9]{32}$`, parts[2])

		address, encodedSignature, ok := strings.Cut(signature, ":")
		require.True(t, ok)
		sig, err := hexutil.Decode(encodedSignature)
		require.NoError(t, err)
		publicKey, err := crypto.SigToPub(accounts.TextHash([]byte(auth)), sig)
		require.NoError(t, err)
		require.Equal(t, crypto.PubkeyToAddress(key.PublicKey), ethcommon.HexToAddress(address))
		require.Equal(t, crypto.PubkeyToAddress(*publicKey), ethcommon.HexToAddress(address))
	}

	receiver := rpcReceiverWithTestTransport(t, receivers[0], server)
	before, err := receiver.auth.headers(time.Now())
	require.NoError(t, err)
	after, err := receiver.auth.headers(before.expiresAt.Add(-30 * time.Second))
	require.NoError(t, err)
	require.NotEqual(t, before.auth, after.auth)
	require.NotEqual(t, before.signature, after.signature)
	require.True(t, after.expiresAt.After(before.expiresAt))
}

func TestRPCReceiverRejectsFailedResponses(t *testing.T) {
	key, keyFile := rpcReceiverTestKey(t)
	tx := rpcReceiverTestTransaction(t, key, 0)
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"HTTP error", http.StatusBadRequest, "response detail"},
		{"RPC error", http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"response detail"}}`},
		{"malformed response", http.StatusOK, "response detail"},
		{"wrong hash", http.StatusOK, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":%q}`, (ethcommon.Hash{}).Hex())},
		{"wrong id", http.StatusOK, fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"result":%q}`, tx.Hash().Hex())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, err := io.WriteString(w, tt.body)
				assert.NoError(t, err)
			}))
			defer server.Close()

			receivers, err := NewRPCReceivers([]string{server.URL}, keyFile)
			require.NoError(t, err)
			receiver := rpcReceiverWithTestTransport(t, receivers[0], server)
			err = receiver.SendTx(t.Context(), &common.TxIn{Tx: tx})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "response detail")
		})
	}
}

func TestRPCReceiverRejectsRedirects(t *testing.T) {
	key, keyFile := rpcReceiverTestKey(t)
	tx := rpcReceiverTestTransaction(t, key, 0)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			t.Error("RPC receiver followed a redirect")
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": tx.Hash()}))
			return
		}
		http.Redirect(w, r, "/redirect", http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	receivers, err := NewRPCReceivers([]string{server.URL}, keyFile)
	require.NoError(t, err)
	receiver := rpcReceiverWithTestTransport(t, receivers[0], server)
	require.Error(t, receiver.SendTx(t.Context(), &common.TxIn{Tx: tx}))
}

func TestNewRPCReceiversRequiresHTTPSAndSigner(t *testing.T) {
	_, keyFile := rpcReceiverTestKey(t)
	_, err := NewRPCReceivers([]string{"http://example.invalid/"}, keyFile)
	require.Error(t, err)
	_, err = NewRPCReceivers([]string{"https://example.invalid/"}, "")
	require.Error(t, err)
}

func rpcReceiverWithTestTransport(t *testing.T, receiver TxReceiver, server *httptest.Server) *rpcReceiver {
	t.Helper()
	rpc, ok := receiver.(*rpcReceiver)
	require.True(t, ok)
	rpc.client.Transport = server.Client().Transport
	return rpc
}

func rpcReceiverTestKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyFile := filepath.Join(t.TempDir(), "signing-key")
	require.NoError(t, os.WriteFile(keyFile, []byte(hexutil.Encode(crypto.FromECDSA(key))+"\n"), 0o600))
	return key, keyFile
}

func rpcReceiverTestTransaction(t *testing.T, key *ecdsa.PrivateKey, nonce uint64) *types.Transaction {
	t.Helper()
	tx, err := types.SignNewTx(key, types.LatestSignerForChainID(big.NewInt(1)), &types.LegacyTx{
		Nonce:    nonce,
		GasPrice: big.NewInt(1),
		Gas:      21000,
		To:       &ethcommon.Address{},
	})
	require.NoError(t, err)
	return tx
}
