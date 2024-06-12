package api

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

type SSESubscription struct {
	uid string
	txC chan string
}

func (s *Server) handleTxSSE(w http.ResponseWriter, r *http.Request) {
	// SSE server for transactions
	s.log.Info("SSE connection opened for transactions")

	// Set CORS headers to allow all origins. You may want to restrict this to specific origins in a production environment.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subscriber := SSESubscription{
		uid: uuid.New().String(),
		txC: make(chan string, 100),
	}
	s.addSubscriber(&subscriber)

	// pingTicker := time.NewTicker(5 * time.Second)

	// Wait for txs or end of request...
	for {
		select {
		case <-r.Context().Done():
			s.log.Info("SSE closed, removing subscriber")
			s.removeSubscriber(&subscriber)
			return

		case tx := <-subscriber.txC:
			fmt.Fprintf(w, "data: %s\n\n", tx)
			w.(http.Flusher).Flush() //nolint:forcetypeassert

			// case <-pingTicker.C:
			// 	fmt.Fprintf(w, ": ping\n\n")
			// 	w.(http.Flusher).Flush() //nolint:forcetypeassert
		}
	}
}
