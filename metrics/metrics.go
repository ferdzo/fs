package metrics

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var defaultBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01,
	0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10,
}

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
	inFlight  atomic.Int64

	mu sync.Mutex

	httpRequests     map[string]uint64
	httpResponseByte map[string]uint64
	httpDuration     map[string]*histogram

	authRequests map[string]uint64

	serviceOps      map[string]uint64
	serviceDuration map[string]*histogram

	dbTxTotal    map[string]uint64
	dbTxDuration map[string]*histogram

	blobOps      map[string]uint64
	blobBytes    map[string]uint64
	blobDuration map[string]*histogram

	gcRuns          map[string]uint64
	gcDuration      *histogram
	gcDeletedChunks uint64
	gcDeleteErrors  uint64
	gcCleanedUpload uint64
}

func NewRegistry() *Registry {
	return &Registry{
		startedAt:        time.Now(),
		httpRequests:     make(map[string]uint64),
		httpResponseByte: make(map[string]uint64),
		httpDuration:     make(map[string]*histogram),
		authRequests:     make(map[string]uint64),
		serviceOps:       make(map[string]uint64),
		serviceDuration:  make(map[string]*histogram),
		dbTxTotal:        make(map[string]uint64),
		dbTxDuration:     make(map[string]*histogram),
		blobOps:          make(map[string]uint64),
		blobBytes:        make(map[string]uint64),
		blobDuration:     make(map[string]*histogram),
		gcRuns:           make(map[string]uint64),
		gcDuration:       newHistogram(defaultBuckets),
	}
}

func (r *Registry) IncHTTPInFlight() {
	r.inFlight.Add(1)
}

func (r *Registry) DecHTTPInFlight() {
	r.inFlight.Add(-1)
}

func (r *Registry) ObserveHTTPRequest(method, route string, status int, d time.Duration, responseBytes int) {
	route = normalizeRoute(route)
	key := method + "|" + route + "|" + strconv.Itoa(status)
	durationKey := method + "|" + route

	r.mu.Lock()
	r.httpRequests[key]++
	if responseBytes > 0 {
		r.httpResponseByte[key] += uint64(responseBytes)
	}
	h := r.httpDuration[durationKey]
	if h == nil {
		h = newHistogram(defaultBuckets)
		r.httpDuration[durationKey] = h
	}
	h.observe(d.Seconds())
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

func (r *Registry) ObserveBlob(operation string, d time.Duration, bytes int64, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	key := operation + "|" + result
	r.mu.Lock()
	r.blobOps[key]++
	h := r.blobDuration[operation]
	if h == nil {
		h = newHistogram(defaultBuckets)
		r.blobDuration[operation] = h
	}
	h.observe(d.Seconds())
	if bytes > 0 {
		switch operation {
		case "read_chunk":
			r.blobBytes["read"] += uint64(bytes)
		case "write_chunk":
			r.blobBytes["write"] += uint64(bytes)
		}
	}
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
	httpReq := copyCounterMap(r.httpRequests)
	httpBytes := copyCounterMap(r.httpResponseByte)
	httpDur := copyHistogramMap(r.httpDuration)
	authReq := copyCounterMap(r.authRequests)
	serviceOps := copyCounterMap(r.serviceOps)
	serviceDur := copyHistogramMap(r.serviceDuration)
	dbTx := copyCounterMap(r.dbTxTotal)
	dbTxDur := copyHistogramMap(r.dbTxDuration)
	blobOps := copyCounterMap(r.blobOps)
	blobBytes := copyCounterMap(r.blobBytes)
	blobDur := copyHistogramMap(r.blobDuration)
	gcRuns := copyCounterMap(r.gcRuns)
	gcDurBounds, gcDurCounts, gcDurSum, gcDurCount := r.gcDuration.snapshot()
	gcDeletedChunks := r.gcDeletedChunks
	gcDeleteErrors := r.gcDeleteErrors
	gcCleanedUploads := r.gcCleanedUpload
	r.mu.Unlock()

	var b strings.Builder

	writeGauge(&b, "fs_http_inflight_requests", "Current in-flight HTTP requests.", float64(r.inFlight.Load()))
	writeCounterVecKV(&b, "fs_http_requests_total", "Total HTTP requests handled.", httpReq, []string{"method", "route", "status"})
	writeCounterVecKV(&b, "fs_http_response_bytes_total", "Total HTTP response bytes written.", httpBytes, []string{"method", "route", "status"})
	writeHistogramVecKV(&b, "fs_http_request_duration_seconds", "HTTP request latency.", httpDur, []string{"method", "route"})

	writeCounterVecKV(&b, "fs_auth_requests_total", "Authentication attempts by result.", authReq, []string{"result", "auth_type", "reason"})

	writeCounterVecKV(&b, "fs_service_operations_total", "Service-level operation calls.", serviceOps, []string{"operation", "result"})
	writeHistogramVecKV(&b, "fs_service_operation_duration_seconds", "Service-level operation latency.", serviceDur, []string{"operation"})

	writeCounterVecKV(&b, "fs_metadata_tx_total", "Metadata transaction calls.", dbTx, []string{"type", "result"})
	writeHistogramVecKV(&b, "fs_metadata_tx_duration_seconds", "Metadata transaction latency.", dbTxDur, []string{"type"})

	writeCounterVecKV(&b, "fs_blob_operations_total", "Blob store operations.", blobOps, []string{"operation", "result"})
	writeCounterVecKV(&b, "fs_blob_bytes_total", "Blob bytes processed.", blobBytes, []string{"direction"})
	writeHistogramVecKV(&b, "fs_blob_operation_duration_seconds", "Blob operation latency.", blobDur, []string{"operation"})

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

	return b.String()
}

func normalizeRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "/unknown"
	}
	return route
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

func copyHistogramMap(src map[string]*histogram) map[string]histogramSnapshot {
	out := make(map[string]histogramSnapshot, len(src))
	for k, h := range src {
		bounds, counts, sum, count := h.snapshot()
		out[k] = histogramSnapshot{
			bounds: bounds,
			counts: counts,
			sum:    sum,
			count:  count,
		}
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
	writeHistogramWithLabelsMap(b, name, labels, histogramSnapshot{
		bounds: bounds,
		counts: counts,
		sum:    sum,
		count:  count,
	})
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
	fmt.Fprintf(b, "%s_sum{%s} %.9f\n", name, labelsToString(labels), s.sum)
	fmt.Fprintf(b, "%s_count{%s} %d\n", name, labelsToString(labels), s.count)
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
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
