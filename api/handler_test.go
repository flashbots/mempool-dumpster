package api

// func getTestLogger() *zap.SugaredLogger {
// 	return common.GetLogger(true, false)
// }

// func Test_Handlers_Healthcheck_Drain_Undrain(t *testing.T) {
// 	const (
// 		latency    = 200 * time.Millisecond
// 		listenAddr = ":8080"
// 	)

// 	//nolint: exhaustruct
// 	// s := New(&HTTPServerConfig{
// 	// 	DrainDuration: latency,
// 	// 	ListenAddr:    listenAddr,
// 	// 	Log:           getTestLogger(),
// 	// })

// 	// { // Check health
// 	// 	req := httptest.NewRequest(http.MethodGet, "http://localhost"+listenAddr+"/readyz", nil) //nolint:goconst,nolintlint
// 	// 	w := httptest.NewRecorder()
// 	// 	s.handleReadinessCheck(w, req)
// 	// 	resp := w.Result()
// 	// 	defer resp.Body.Close()
// 	// 	_, err := io.ReadAll(resp.Body)
// 	// 	require.NoError(t, err)
// 	// 	require.Equal(t, http.StatusOK, resp.StatusCode, "Healthcheck must return `Ok` before draining")
// 	// }
// }
