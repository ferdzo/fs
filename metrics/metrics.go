package metrics

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var defaultBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01,
	0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10,
}

var lockBuckets = []float64{
	0.000001, 0.000005, 0.00001, 0.00005,
	0.0001, 0.0005, 0.001, 0.005, 0.01,
	0.025, 0.05, 0.1, 0.25, 0.5, 1,
}

var batchBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 100, 128, 256, 512, 1000, 5000}

var Default = NewRegistry()

type histogram struct {
	bounds []float64
	counts []uint64
	sum    float64
	count  uint64
}

func newHistogram(bounds []float64) *histogram {
	cloned := make([]float64, len(bounds))
	copy(cloned, bounds)
	return &histogram{
		bounds: cloned,
		counts: make([]uint64, len(bounds)+1),
	}
}

func (h *histogram) observe(v float64) {
	h.count++
	h.sum += v
	for i, bound := range h.bounds {
		if v <= bound {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.counts)-1]++
}

func (h *histogram) snapshot() (bounds []float64, counts []uint64, sum float64, count uint64) {
	bounds = make([]float64, len(h.bounds))
	copy(bounds, h.bounds)
	counts = make([]uint64, len(h.counts))
	copy(counts, h.counts)
	return bounds, counts, h.sum, h.count
}

type Registry struct {
	startedAt time.Time

	httpInFlight atomic.Int64

	connectionPoolActive atomic.Int64
	connectionPoolMax    atomic.Int64
	connectionPoolWaits  atomic.Uint64

	requestQueueLength atomic.Int64

	mu sync.Mutex

	httpRequestsRoute      map[string]uint64
	httpResponseBytesRoute map[string]uint64
	httpDurationRoute      map[string]*histogram

	httpRequestsOp map[string]uint64
	httpDurationOp map[string]*histogram
	httpInFlightOp map[string]int64

	authRequests map[string]uint64

	serviceOps      map[string]uint64
	serviceDuration map[string]*histogram

	dbTxTotal    map[string]uint64
	dbTxDuration map[string]*histogram

	blobOps      map[string]uint64
	blobBytes    map[string]uint64
	blobDuration map[string]*histogram

	lockWait map[string]*histogram
	lockHold map[string]*histogram

	cacheHits   map[string]uint64
	cacheMisses map[string]uint64

	batchSize *histogram

	retries map[string]uint64
	errors  map[string]uint64

	gcRuns          map[string]uint64
	gcDuration      *histogram
	gcDeletedChunks uint64
	gcDeleteErrors  uint64
	gcCleanedUpload uint64
}

func NewRegistry() *Registry {
	return &Registry{
		startedAt:              time.Now(),
		httpRequestsRoute:      make(map[string]uint64),
		httpResponseBytesRoute: make(map[string]uint64),
		httpDurationRoute:      make(map[string]*histogram),
		httpRequestsOp:         make(map[string]uint64),
		httpDurationOp:         make(map[string]*histogram),
		httpInFlightOp:         make(map[string]int64),
		authRequests:           make(map[string]uint64),
		serviceOps:             make(map[string]uint64),
		serviceDuration:        make(map[string]*histogram),
		dbTxTotal:              make(map[string]uint64),
		dbTxDuration:           make(map[string]*histogram),
		blobOps:                make(map[string]uint64),
		blobBytes:              make(map[string]uint64),
		blobDuration:           make(map[string]*histogram),
		lockWait:               make(map[string]*histogram),
		lockHold:               make(map[string]*histogram),
		cacheHits:              make(map[string]uint64),
		cacheMisses:            make(map[string]uint64),
		batchSize:              newHistogram(batchBuckets),
		retries:                make(map[string]uint64),
		errors:                 make(map[string]uint64),
		gcRuns:                 make(map[string]uint64),
		gcDuration:             newHistogram(defaultBuckets),
	}
}

func NormalizeHTTPOperation(method string, isDeletePost bool) string {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "GET":
		return "get"
	case "PUT":
		return "put"
	case "DELETE":
		return "delete"
	case "HEAD":
		return "head"
	case "POST":
		if isDeletePost {
			return "delete"
		}
		return "put"
	default:
		return "other"
	}
}

func statusResult(status int) string {
	if status >= 200 && status < 400 {
		return "ok"
	}
	return "error"
}

func normalizeRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "/unknown"
	}
	return route
}

func normalizeOp(op string) string {
	op = strings.ToLower(strings.TrimSpace(op))
	if op == "" {
		return "other"
	}
	return op
}

func (r *Registry) IncHTTPInFlight() {
	r.httpInFlight.Add(1)
}

func (r *Registry) DecHTTPInFlight() {
	r.httpInFlight.Add(-1)
}

func (r *Registry) IncHTTPInFlightOp(op string) {
	r.httpInFlight.Add(1)
	op = normalizeOp(op)
	r.mu.Lock()
	r.httpInFlightOp[op]++
	r.mu.Unlock()
}

func (r *Registry) DecHTTPInFlightOp(op string) {
	r.httpInFlight.Add(-1)
	op = normalizeOp(op)
	r.mu.Lock()
	r.httpInFlightOp[op]--
	if r.httpInFlightOp[op] < 0 {
		r.httpInFlightOp[op] = 0
	}
	r.mu.Unlock()
}

func (r *Registry) ObserveHTTPRequest(method, route string, status int, d time.Duration, responseBytes int) {
	op := NormalizeHTTPOperation(method, false)
	r.ObserveHTTPRequestDetailed(method, route, op, status, d, responseBytes)
}

func (r *Registry) ObserveHTTPRequestDetailed(method, route, op string, status int, d time.Duration, responseBytes int) {
	route = normalizeRoute(route)
	op = normalizeOp(op)
	result := statusResult(status)

	routeKey := method + "|" + route + "|" + strconv.Itoa(status)
	routeDurKey := method + "|" + route
	opKey := op + "|" + result

	r.mu.Lock()
	r.httpRequestsRoute[routeKey]++
	if responseBytes > 0 {
		r.httpResponseBytesRoute[routeKey] += uint64(responseBytes)
	}
	hRoute := r.httpDurationRoute[routeDurKey]
	if hRoute == nil {
		hRoute = newHistogram(defaultBuckets)
		r.httpDurationRoute[routeDurKey] = hRoute
	}
	hRoute.observe(d.Seconds())

	r.httpRequestsOp[opKey]++
	hOp := r.httpDurationOp[opKey]
	if hOp == nil {
		hOp = newHistogram(defaultBuckets)
		r.httpDurationOp[opKey] = hOp
	}
	hOp.observe(d.Seconds())
	r.mu.Unlock()
}

func (r *Registry) ObserveAuth(result, authType, reason string) {
	authType = strings.TrimSpace(authType)
	if authType == "" {
		authType = "unknown"
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "none"
	}
	key := result + "|" + authType + "|" + reason
	r.mu.Lock()
	r.authRequests[key]++
	r.mu.Unlock()
}

func (r *Registry) ObserveService(operation string, d time.Duration, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	key := operation + "|" + result
	r.mu.Lock()
	r.serviceOps[key]++
	h := r.serviceDuration[operation]
	if h == nil {
		h = newHistogram(defaultBuckets)
		r.serviceDuration[operation] = h
	}
	h.observe(d.Seconds())
	r.mu.Unlock()
}

func (r *Registry) ObserveMetadataTx(txType string, d time.Duration, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	key := txType + "|" + result
	r.mu.Lock()
	r.dbTxTotal[key]++
	h := r.dbTxDuration[txType]
	if h == nil {
		h = newHistogram(defaultBuckets)
		r.dbTxDuration[txType] = h
	}
	h.observe(d.Seconds())
	r.mu.Unlock()
}

func (r *Registry) ObserveBlob(operation string, d time.Duration, bytes int64, ok bool, backend ...string) {
	be := "disk"
	if len(backend) > 0 {
		candidate := strings.TrimSpace(backend[0])
		if candidate != "" {
			be = strings.ToLower(candidate)
		}
	}
	result := "error"
	if ok {
		result = "ok"
	}
	op := strings.ToLower(strings.TrimSpace(operation))
	if op == "" {
		op = "unknown"
	}

	histKey := op + "|" + be + "|" + result
	opsKey := histKey

	r.mu.Lock()
	r.blobOps[opsKey]++
	h := r.blobDuration[histKey]
	if h == nil {
		h = newHistogram(defaultBuckets)
		r.blobDuration[histKey] = h
	}
	h.observe(d.Seconds())

	if bytes > 0 {
		r.blobBytes[op] += uint64(bytes)
	}
	r.mu.Unlock()
}

func (r *Registry) SetConnectionPoolMax(max int) {
	if max < 0 {
		max = 0
	}
	r.connectionPoolMax.Store(int64(max))
}

func (r *Registry) IncConnectionPoolActive() {
	r.connectionPoolActive.Add(1)
}

func (r *Registry) DecConnectionPoolActive() {
	r.connectionPoolActive.Add(-1)
}

func (r *Registry) IncConnectionPoolWait() {
	r.connectionPoolWaits.Add(1)
}

func (r *Registry) IncRequestQueueLength() {
	r.requestQueueLength.Add(1)
}

func (r *Registry) DecRequestQueueLength() {
	r.requestQueueLength.Add(-1)
}

func (r *Registry) ObserveLockWait(lockName string, d time.Duration) {
	lockName = strings.TrimSpace(lockName)
	if lockName == "" {
		lockName = "unknown"
	}
	r.mu.Lock()
	h := r.lockWait[lockName]
	if h == nil {
		h = newHistogram(lockBuckets)
		r.lockWait[lockName] = h
	}
	h.observe(d.Seconds())
	r.mu.Unlock()
}

func (r *Registry) ObserveLockHold(lockName string, d time.Duration) {
	lockName = strings.TrimSpace(lockName)
	if lockName == "" {
		lockName = "unknown"
	}
	r.mu.Lock()
	h := r.lockHold[lockName]
	if h == nil {
		h = newHistogram(lockBuckets)
		r.lockHold[lockName] = h
	}
	h.observe(d.Seconds())
	r.mu.Unlock()
}

func (r *Registry) ObserveCacheHit(cache string) {
	cache = strings.TrimSpace(cache)
	if cache == "" {
		cache = "unknown"
	}
	r.mu.Lock()
	r.cacheHits[cache]++
	r.mu.Unlock()
}

func (r *Registry) ObserveCacheMiss(cache string) {
	cache = strings.TrimSpace(cache)
	if cache == "" {
		cache = "unknown"
	}
	r.mu.Lock()
	r.cacheMisses[cache]++
	r.mu.Unlock()
}

func (r *Registry) ObserveBatchSize(size int) {
	if size < 0 {
		size = 0
	}
	r.mu.Lock()
	r.batchSize.observe(float64(size))
	r.mu.Unlock()
}

func (r *Registry) ObserveRetry(op, reason string) {
	op = normalizeOp(op)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	key := op + "|" + reason
	r.mu.Lock()
	r.retries[key]++
	r.mu.Unlock()
}

func (r *Registry) ObserveError(op, reason string) {
	op = normalizeOp(op)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	key := op + "|" + reason
	r.mu.Lock()
	r.errors[key]++
	r.mu.Unlock()
}

func (r *Registry) ObserveGC(d time.Duration, deletedChunks, deleteErrors, cleanedUploads int, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	r.mu.Lock()
	r.gcRuns[result]++
	r.gcDuration.observe(d.Seconds())
	if deletedChunks > 0 {
		r.gcDeletedChunks += uint64(deletedChunks)
	}
	if deleteErrors > 0 {
		r.gcDeleteErrors += uint64(deleteErrors)
	}
	if cleanedUploads > 0 {
		r.gcCleanedUpload += uint64(cleanedUploads)
	}
	r.mu.Unlock()
}

func (r *Registry) RenderPrometheus() string {
	now := time.Now()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	r.mu.Lock()
	httpReqRoute := copyCounterMap(r.httpRequestsRoute)
	httpRespRoute := copyCounterMap(r.httpResponseBytesRoute)
	httpDurRoute := copyHistogramMap(r.httpDurationRoute)
	httpReqOp := copyCounterMap(r.httpRequestsOp)
	httpDurOp := copyHistogramMap(r.httpDurationOp)
	httpInFlightOp := copyIntGaugeMap(r.httpInFlightOp)
	authReq := copyCounterMap(r.authRequests)
	serviceOps := copyCounterMap(r.serviceOps)
	serviceDur := copyHistogramMap(r.serviceDuration)
	dbTx := copyCounterMap(r.dbTxTotal)
	dbTxDur := copyHistogramMap(r.dbTxDuration)
	blobOps := copyCounterMap(r.blobOps)
	blobBytes := copyCounterMap(r.blobBytes)
	blobDur := copyHistogramMap(r.blobDuration)
	lockWait := copyHistogramMap(r.lockWait)
	lockHold := copyHistogramMap(r.lockHold)
	cacheHits := copyCounterMap(r.cacheHits)
	cacheMisses := copyCounterMap(r.cacheMisses)
	batchBounds, batchCounts, batchSum, batchCount := r.batchSize.snapshot()
	retries := copyCounterMap(r.retries)
	errorsTotal := copyCounterMap(r.errors)
	gcRuns := copyCounterMap(r.gcRuns)
	gcDurBounds, gcDurCounts, gcDurSum, gcDurCount := r.gcDuration.snapshot()
	gcDeletedChunks := r.gcDeletedChunks
	gcDeleteErrors := r.gcDeleteErrors
	gcCleanedUploads := r.gcCleanedUpload
	r.mu.Unlock()

	connectionActive := float64(r.connectionPoolActive.Load())
	connectionMax := float64(r.connectionPoolMax.Load())
	connectionWaits := r.connectionPoolWaits.Load()
	queueLength := float64(r.requestQueueLength.Load())

	resident, hasResident := readResidentMemoryBytes()
	cpuSeconds, hasCPU := readProcessCPUSeconds()

	var b strings.Builder

	httpInFlightOp["all"] = r.httpInFlight.Load()
	writeGaugeVecFromInt64(&b, "fs_http_inflight_requests", "Current in-flight HTTP requests by operation.", httpInFlightOp, "op")
	writeCounterVecKV(&b, "fs_http_requests_total", "Total HTTP requests by operation and result.", httpReqOp, []string{"op", "result"})
	writeHistogramVecKV(&b, "fs_http_request_duration_seconds", "HTTP request latency by operation and result.", httpDurOp, []string{"op", "result"})

	writeCounterVecKV(&b, "fs_http_requests_by_route_total", "Total HTTP requests by method/route/status.", httpReqRoute, []string{"method", "route", "status"})
	writeCounterVecKV(&b, "fs_http_response_bytes_total", "Total HTTP response bytes written.", httpRespRoute, []string{"method", "route", "status"})
	writeHistogramVecKV(&b, "fs_http_request_duration_by_route_seconds", "HTTP request latency by method/route.", httpDurRoute, []string{"method", "route"})

	writeCounterVecKV(&b, "fs_auth_requests_total", "Authentication attempts by result.", authReq, []string{"result", "auth_type", "reason"})

	writeCounterVecKV(&b, "fs_service_operations_total", "Service-level operation calls.", serviceOps, []string{"operation", "result"})
	writeHistogramVecKV(&b, "fs_service_operation_duration_seconds", "Service-level operation latency.", serviceDur, []string{"operation"})

	writeCounterVecKV(&b, "fs_metadata_tx_total", "Metadata transaction calls.", dbTx, []string{"type", "result"})
	writeHistogramVecKV(&b, "fs_metadata_tx_duration_seconds", "Metadata transaction latency.", dbTxDur, []string{"type"})

	writeHistogramVecKV(&b, "fs_blob_operation_duration_seconds", "Blob backend operation latency.", blobDur, []string{"op", "backend", "result"})
	writeCounterVecKV(&b, "fs_blob_operations_total", "Blob store operations.", blobOps, []string{"op", "backend", "result"})
	writeCounterVecKV(&b, "fs_blob_bytes_total", "Blob bytes processed by operation.", blobBytes, []string{"op"})

	writeGauge(&b, "fs_connection_pool_active", "Active pooled connections.", connectionActive)
	writeGauge(&b, "fs_connection_pool_max", "Maximum pooled connections.", connectionMax)
	writeCounter(&b, "fs_connection_pool_waits_total", "Number of waits due to pool saturation.", connectionWaits)

	writeGauge(&b, "fs_request_queue_length", "Requests waiting for an execution slot.", queueLength)

	writeHistogramVecKV(&b, "fs_lock_wait_seconds", "Time spent waiting for locks.", lockWait, []string{"lock_name"})
	writeHistogramVecKV(&b, "fs_lock_hold_seconds", "Time locks were held.", lockHold, []string{"lock_name"})

	writeCounterVecKV(&b, "fs_cache_hits_total", "Cache hits by cache name.", cacheHits, []string{"cache"})
	writeCounterVecKV(&b, "fs_cache_misses_total", "Cache misses by cache name.", cacheMisses, []string{"cache"})

	writeHistogram(&b, "fs_batch_size_histogram", "Observed batch sizes.", nil, batchBounds, batchCounts, batchSum, batchCount)

	writeCounterVecKV(&b, "fs_retries_total", "Retries by operation and reason.", retries, []string{"op", "reason"})
	writeCounterVecKV(&b, "fs_errors_total", "Errors by operation and reason.", errorsTotal, []string{"op", "reason"})

	writeCounterVecKV(&b, "fs_gc_runs_total", "Garbage collection runs.", gcRuns, []string{"result"})
	writeHistogram(&b, "fs_gc_duration_seconds", "Garbage collection runtime.", nil, gcDurBounds, gcDurCounts, gcDurSum, gcDurCount)
	writeCounter(&b, "fs_gc_deleted_chunks_total", "Deleted chunks during GC.", gcDeletedChunks)
	writeCounter(&b, "fs_gc_delete_errors_total", "Chunk delete errors during GC.", gcDeleteErrors)
	writeCounter(&b, "fs_gc_cleaned_uploads_total", "Cleaned multipart uploads during GC.", gcCleanedUploads)

	writeGauge(&b, "fs_uptime_seconds", "Process uptime in seconds.", now.Sub(r.startedAt).Seconds())
	writeGauge(&b, "fs_runtime_goroutines", "Number of goroutines.", float64(runtime.NumGoroutine()))
	writeGaugeVec(&b, "fs_runtime_memory_bytes", "Runtime memory in bytes.", map[string]float64{
		"alloc":      float64(mem.Alloc),
		"total":      float64(mem.TotalAlloc),
		"sys":        float64(mem.Sys),
		"heap_alloc": float64(mem.HeapAlloc),
		"heap_sys":   float64(mem.HeapSys),
		"stack_sys":  float64(mem.StackSys),
	}, "type")
	writeCounter(&b, "fs_runtime_gc_cycles_total", "Completed GC cycles.", uint64(mem.NumGC))
	writeCounterFloat(&b, "fs_runtime_gc_pause_seconds_total", "Total GC pause time in seconds.", float64(mem.PauseTotalNs)/1e9)

	if hasCPU {
		writeCounterFloat(&b, "process_cpu_seconds_total", "Total user and system CPU time spent in seconds.", cpuSeconds)
	}
	if hasResident {
		writeGauge(&b, "process_resident_memory_bytes", "Resident memory size in bytes.", resident)
	}

	return b.String()
}

type histogramSnapshot struct {
	bounds []float64
	counts []uint64
	sum    float64
	count  uint64
}

func copyCounterMap(src map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func copyIntGaugeMap(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func copyHistogramMap(src map[string]*histogram) map[string]histogramSnapshot {
	out := make(map[string]histogramSnapshot, len(src))
	for k, h := range src {
		bounds, counts, sum, count := h.snapshot()
		out[k] = histogramSnapshot{bounds: bounds, counts: counts, sum: sum, count: count}
	}
	return out
}

func writeCounter(b *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}

func writeCounterFloat(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %.9f\n", name, value)
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %.9f\n", name, value)
}

func writeGaugeVec(b *strings.Builder, name, help string, values map[string]float64, labelName string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(b, "%s{%s=\"%s\"} %.9f\n", name, labelName, escapeLabelValue(key), values[key])
	}
}

func writeGaugeVecFromInt64(b *strings.Builder, name, help string, values map[string]int64, labelName string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(b, "%s{%s=\"%s\"} %.9f\n", name, labelName, escapeLabelValue(key), float64(values[key]))
	}
}

func writeCounterVecKV(b *strings.Builder, name, help string, values map[string]uint64, labels []string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.Split(key, "|")
		fmt.Fprintf(b, "%s{%s} %d\n", name, formatLabels(labels, parts), values[key])
	}
}

func writeHistogramVecKV(b *strings.Builder, name, help string, values map[string]histogramSnapshot, labels []string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s histogram\n", name)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.Split(key, "|")
		labelsMap := make(map[string]string, len(labels))
		for i, label := range labels {
			if i < len(parts) {
				labelsMap[label] = parts[i]
			} else {
				labelsMap[label] = ""
			}
		}
		writeHistogramWithLabelsMap(b, name, labelsMap, values[key])
	}
}

func writeHistogram(b *strings.Builder, name, help string, labels map[string]string, bounds []float64, counts []uint64, sum float64, count uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s histogram\n", name)
	writeHistogramWithLabelsMap(b, name, labels, histogramSnapshot{bounds: bounds, counts: counts, sum: sum, count: count})
}

func writeHistogramWithLabelsMap(b *strings.Builder, name string, labels map[string]string, s histogramSnapshot) {
	var cumulative uint64
	for i, bucketCount := range s.counts {
		cumulative += bucketCount
		bucketLabels := cloneLabels(labels)
		if i < len(s.bounds) {
			bucketLabels["le"] = trimFloat(s.bounds[i])
		} else {
			bucketLabels["le"] = "+Inf"
		}
		fmt.Fprintf(b, "%s_bucket{%s} %d\n", name, labelsToString(bucketLabels), cumulative)
	}
	labelsSuffix := formatLabelsSuffix(labels)
	fmt.Fprintf(b, "%s_sum%s %.9f\n", name, labelsSuffix, s.sum)
	fmt.Fprintf(b, "%s_count%s %d\n", name, labelsSuffix, s.count)
}

func formatLabelsSuffix(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	return "{" + labelsToString(labels) + "}"
}

func formatLabels(keys, values []string) string {
	parts := make([]string, 0, len(keys))
	for i, key := range keys {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", key, escapeLabelValue(value)))
	}
	return strings.Join(parts, ",")
}

func labelsToString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", key, escapeLabelValue(labels[key])))
	}
	return strings.Join(parts, ",")
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\\n`)
	value = strings.ReplaceAll(value, `"`, `\\"`)
	return value
}

func readResidentMemoryBytes() (float64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	rssPages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || rssPages < 0 {
		return 0, false
	}
	return float64(rssPages * int64(os.Getpagesize())), true
}

func readProcessCPUSeconds() (float64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	user := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	sys := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	return user + sys, true
}
