package ingest

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"webhook-relay/internal/drainrate"
	"webhook-relay/internal/idempotency"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

// IngestRequest 是 POST /v1/events 的请求体。
type IngestRequest struct {
	ProducerKey string          `json:"producer_key,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// Handler 是 ingest 端点：校验租户 → 生成幂等键 → 有界入队 → 持久化任务记录。
type Handler struct {
	keys       *idempotency.KeyGenerator
	newTaskID  func() string
	queue      queue.Queue
	tasks      state.TaskStore
	retryAfter RetryAfterCalc
	dr         *drainrate.Estimator
	ttl        time.Duration
	registry   tenant.Registry
	m          *metrics.Metrics
}

func NewHandler(keys *idempotency.KeyGenerator, q queue.Queue, s state.TaskStore, registry tenant.Registry, m *metrics.Metrics, dr *drainrate.Estimator) *Handler {
	return &Handler{
		keys:       keys,
		newTaskID:  uuid.NewString,
		queue:      q,
		tasks:      s,
		retryAfter: ClampingRetryAfter{Min: time.Second, Max: time.Minute},
		ttl:        25 * time.Hour,
		registry:   registry,
		m:          m,
		dr:         dr,
	}
}

// drainRate 返回租户的 EWMA 成功投递速率；未接 estimator 时回退 1.0。
func (h *Handler) drainRate(tenantID string) float64 {
	if h.dr != nil {
		return h.dr.Rate(tenantID)
	}
	return 1.0
}

func (h *Handler) ingestTotal(tenantID, status string) {
	if h.m != nil {
		h.m.IngestTotal.WithLabelValues(tenantID, status).Inc()
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		h.ingestTotal("", "400")
		http.Error(w, "missing X-Tenant-ID", http.StatusBadRequest)
		return
	}
	if h.registry != nil {
		if _, err := h.registry.Get(r.Context(), tenantID); err != nil {
			h.ingestTotal(tenantID, "404")
			http.Error(w, "unknown tenant", http.StatusNotFound)
			return
		}
	}

	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.ingestTotal(tenantID, "400")
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	key := h.keys.Generate(tenantID, req.ProducerKey)
	taskID := state.TaskID(h.newTaskID())

	if err := h.queue.Enqueue(r.Context(), tenantID, taskID); err != nil {
		if errors.Is(err, queue.ErrQueueFull) {
			depth, derr := h.queue.Depth(r.Context(), tenantID)
			if derr != nil {
				h.ingestTotal(tenantID, "503")
				http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
				return
			}
			retryAfter := h.retryAfter.Compute(depth, h.drainRate(tenantID))
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter/time.Second)))
			h.ingestTotal(tenantID, "429")
			if h.m != nil {
				h.m.Ingest429.WithLabelValues(tenantID).Inc()
			}
			http.Error(w, "tenant queue full", http.StatusTooManyRequests)
			return
		}
		h.ingestTotal(tenantID, "503")
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}

	task := state.Task{
		ID:             taskID,
		TenantID:       tenantID,
		IdempotencyKey: key,
		Payload:        []byte(req.Payload),
		ContentType:    r.Header.Get("Content-Type"),
		Status:         state.StatusEnqueued,
		IngestedAt:     time.Now(),
	}
	if err := h.tasks.Put(r.Context(), task, h.ttl); err != nil {
		h.ingestTotal(tenantID, "503")
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}

	h.ingestTotal(tenantID, "202")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"task_id":         string(taskID),
		"idempotency_key": string(key),
	})
}
