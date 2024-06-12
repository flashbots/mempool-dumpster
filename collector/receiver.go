package collector

//
// Forwarding select transactions to various kinds of receivers.
//
// One type of receiver is HTTPReceiver here.
// Another type is the API server, to stream out transactions as SSE stream.
//

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/flashbots/mempool-dumpster/common"
)

type TxReceiver interface {
	SendTx(ctx context.Context, tx *common.TxIn) error
}

type HTTPReceiver struct {
	url string
}

func NewHTTPReceiver(url string) *HTTPReceiver {
	return &HTTPReceiver{
		url: url,
	}
}

func (r *HTTPReceiver) SendTx(ctx context.Context, tx *common.TxIn) error {
	rawTx, err := tx.Tx.MarshalBinary()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(rawTx))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, res.Body)
	return err
}
