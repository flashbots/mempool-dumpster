// Package website contains the service delivering the website
package website

import (
	"encoding/json"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/gorilla/mux"
	"github.com/tdewolff/minify"
	"github.com/tdewolff/minify/html"
	uberatomic "go.uber.org/atomic"
	"go.uber.org/zap"
)

var ErrServerAlreadyStarted = errors.New("server was already started")

type WebserverOpts struct {
	ListenAddress string
	Log           *zap.SugaredLogger
	Dev           bool // reloads template on every request
	EnablePprof   bool
	// Only24h       bool
}

type Webserver struct {
	opts *WebserverOpts
	log  *zap.SugaredLogger

	srv        *http.Server
	srvStarted uberatomic.Bool
	minifier   *minify.M

	// templateIndex      *template.Template
	// templateDailyStats *template.Template
}

func NewWebserver(opts *WebserverOpts) (server *Webserver, err error) {
	minifier := minify.New()
	minifier.AddFunc("text/css", html.Minify)
	minifier.AddFunc("text/html", html.Minify)
	minifier.AddFunc("application/javascript", html.Minify)

	server = &Webserver{
		opts:     opts,
		log:      opts.Log,
		minifier: minifier,
	}

	// server.templateDailyStats, err = ParseDailyStatsTemplate()
	// if err != nil {
	// 	return nil, err
	// }

	// server.templateIndex, err = ParseIndexTemplate()
	// if err != nil {
	// 	return nil, err
	// }

	return server, nil
}

func (srv *Webserver) StartServer() (err error) {
	if srv.srvStarted.Swap(true) {
		return ErrServerAlreadyStarted
	}

	srv.srv = &http.Server{
		Addr:    srv.opts.ListenAddress,
		Handler: srv.getRouter(),

		ReadTimeout:       600 * time.Millisecond,
		ReadHeaderTimeout: 400 * time.Millisecond,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       3 * time.Second,
	}

	err = srv.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (srv *Webserver) getRouter() http.Handler {
	r := mux.NewRouter()
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("./website/static"))))

	r.HandleFunc("/", srv.handleRoot).Methods(http.MethodGet)
	r.HandleFunc("/index.html", srv.handleRoot).Methods(http.MethodGet)
	r.HandleFunc("/ethereum/mainnet/{month}/index.html", srv.handleMonth).Methods(http.MethodGet)

	if srv.opts.EnablePprof {
		srv.log.Info("pprof API enabled")
		r.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	}
	loggedRouter := LoggingMiddlewareZap(srv.log.Desugar(), r)
	withGz := gziphandler.GzipHandler(loggedRouter)
	return withGz
}

func (srv *Webserver) RespondError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := HTTPErrorResp{code, message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		srv.log.With("response", resp).Errorw("Couldn't write error response", "error", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (srv *Webserver) RespondOK(w http.ResponseWriter, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		srv.log.With("response", response).Errorw("Couldn't write OK response", "error", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (srv *Webserver) handleRoot(w http.ResponseWriter, req *http.Request) {
	// timespan := req.URL.Query().Get("t")

	tpl, err := ParseIndexTemplate()
	if err != nil {
		srv.log.Error("wroot: error parsing template", "error", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = tpl.ExecuteTemplate(w, "base", DummyHTMLData)
	if err != nil {
		srv.log.Error("wroot: error executing template", "error", err)
		return
	}

	// production flow...
	// htmlBuf := bytes.Buffer{}

	// // Render template
	// if err := srv.templateIndex.ExecuteTemplate(&htmlBuf, "base", htmlData); err != nil {
	// 	srv.log.WithError(err).Error("error rendering template")
	// 	srv.RespondError(w, http.StatusInternalServerError, "error rendering template")
	// 	return
	// }

	// // Minify
	// htmlBytes, err := srv.minifier.Bytes("text/html", htmlBuf.Bytes())
	// if err != nil {
	// 	srv.log.WithError(err).Error("error minifying html")
	// 	srv.RespondError(w, http.StatusInternalServerError, "error minifying html")
	// 	return
	// }

	// w.WriteHeader(http.StatusOK)
	// _, _ = w.Write(htmlBytes)
}

// func (srv *Webserver) handleStatsAPI(w http.ResponseWriter, req *http.Request) {
// 	srv.statsAPIRespLock.RLock()
// 	defer srv.statsAPIRespLock.RUnlock()
// 	_, _ = w.Write(*srv.statsAPIResp)
// }

func (srv *Webserver) handleMonth(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	layout := "2006-01"
	t, err := time.Parse(layout, vars["month"])
	if err != nil {
		srv.RespondError(w, http.StatusBadRequest, "invalid date")
		return
	}
	_ = t

	tpl, err := ParseFilesTemplate()
	if err != nil {
		srv.log.Error("wroot: error parsing template", "error", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	err = tpl.ExecuteTemplate(w, "base", DummyHTMLData)
	if err != nil {
		srv.log.Error("wroot: error executing template", "error", err)
		return
	}
}
