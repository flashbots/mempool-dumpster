package website

import (
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	// Printer for pretty printing numbers
	printer = message.NewPrinter(language.English)

	// Caser is used for casing strings
	caser = cases.Title(language.English)
)

type HTTPErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// responseWriter is a minimal wrapper for http.ResponseWriter that allows the
// written HTTP status code to be captured for logging.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w} //nolint:exhaustruct
}

func (rw *responseWriter) Status() int {
	return rw.status
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}

	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
	rw.wroteHeader = true
}

// LoggingMiddlewareZap logs the incoming HTTP request & its duration.
func LoggingMiddlewareZap(logger *zap.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle panics
		defer func() {
			if msg := recover(); msg != nil {
				w.WriteHeader(http.StatusInternalServerError)
				var method, url string
				if r != nil {
					method = r.Method
					url = r.URL.EscapedPath()
				}
				logger.Error("HTTP request handler panicked",
					zap.Any("error", msg),
					zap.String("method", method),
					zap.String("url", url),
				)
			}
		}()

		start := time.Now()
		wrapped := wrapResponseWriter(w)
		next.ServeHTTP(w, r)

		// Passing request stats both in-message (for the human reader)
		// as well as inside the structured log (for the machine parser)
		logger.Info(fmt.Sprintf("%s %s %d", r.Method, r.URL.EscapedPath(), wrapped.status),
			zap.Int("durationMs", int(time.Since(start).Milliseconds())),
			zap.Int("status", wrapped.status),
			zap.String("logType", "access"),
			zap.String("method", r.Method),
			zap.String("path", r.URL.EscapedPath()),
		)
	})
}
