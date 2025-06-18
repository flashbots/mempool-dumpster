// Package api contains the webserver for API and SSE subscription
package api

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/flashbots/go-utils/httplogger"
	"github.com/flashbots/mempool-dumpster/common"
	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type HTTPServerConfig struct {
	ListenAddr string
	Log        *zap.SugaredLogger

	DrainDuration            time.Duration
	GracefulShutdownDuration time.Duration
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
}

type Server struct {
	cfg     *HTTPServerConfig
	isReady atomic.Bool
	log     *zap.SugaredLogger

	srv               *http.Server
	sseConnectionMap  map[string]*SSESubscription
	sseConnectionLock sync.RWMutex
}

func New(cfg *HTTPServerConfig) (srv *Server) {
	srv = &Server{
		cfg:              cfg,
		log:              cfg.Log,
		srv:              nil,
		sseConnectionMap: make(map[string]*SSESubscription),
	}
	srv.isReady.Swap(true)

	mux := chi.NewRouter()

	mux.Use(srv.httpLogger)
	mux.Get("/sse/transactions", srv.handleTxSSE)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv.srv = &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return srv
}

func (s *Server) httpLogger(next http.Handler) http.Handler {
	return httplogger.LoggingMiddlewareZap(s.log.Desugar(), next)
}

func (s *Server) RunInBackground() {
	// api
	go func() {
		s.log.With("listenAddress", s.cfg.ListenAddr).Info("Starting HTTP server")
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.With("err", err).Error("HTTP server failed")
		}
	}()
}

func (s *Server) Shutdown() {
	// api
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.GracefulShutdownDuration)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		s.log.With("err", err).Error("Graceful HTTP server shutdown failed")
	} else {
		s.log.Info("HTTP server gracefully stopped")
	}
}

func (s *Server) addSubscriber(sub *SSESubscription) {
	s.sseConnectionLock.Lock()
	defer s.sseConnectionLock.Unlock()
	s.sseConnectionMap[sub.uid] = sub
}

func (s *Server) removeSubscriber(sub *SSESubscription) {
	s.sseConnectionLock.Lock()
	defer s.sseConnectionLock.Unlock()
	delete(s.sseConnectionMap, sub.uid)
	s.log.With("subscribers", len(s.sseConnectionMap)).Info("removed subscriber")
}

func (s *Server) SendTx(ctx context.Context, tx *common.TxIn) error {
	s.sseConnectionLock.RLock()
	defer s.sseConnectionLock.RUnlock()
	if len(s.sseConnectionMap) == 0 {
		return nil
	}

	txRLP, err := common.TxToRLPString(tx.Tx)
	if err != nil {
		return err
	}

	// Send tx to all subscribers (only if channel is not full)
	for _, sub := range s.sseConnectionMap {
		select {
		case sub.txC <- txRLP:
		default:
		}
	}

	return nil
}
