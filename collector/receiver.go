package collector

import (
	"bytes"
	"context"
	"net/http"
)

type TxReceiver interface {
	SendTx(ctx context.Context, tx *TxIn) error
}

type HTTPReceiver struct {
	url string
}

func NewHTTPReceiver(url string) *HTTPReceiver {
	return &HTTPReceiver{
		url: url,
	}
}

func (r *HTTPReceiver) SendTx(ctx context.Context, tx *TxIn) error {
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
	return err
}
