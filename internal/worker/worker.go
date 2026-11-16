package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"webhook-relay/internal/backoff"
	"webhook-relay/internal/delayed"
	"webhook-relay/internal/dlq"
	"webhook-relay/internal/drainrate"
	"webhook-relay/internal/limiter"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/queue"
	"webhook-relay/internal/state"
	"webhook-relay/internal/tenant"
)

// Worker 是一个租户的投递 worker：取任务、投递、按退避调度重试或进 DLQ。
type Worker struct {
	tenant      tenant.Tenant
	queue       queue.Queue
	tasks       state.TaskStore
	sm          state.StateMachine
	limiter     *limiter.Limiter
	client      *http.Client
	backoff     backoff.Backoff
	delayed     delayed.DelayedQueue
	dlq         dlq.DeadLetterQueue
	rng         *rand.Rand
	leaseDur    time.Duration
	renewEvery  time.Duration
	m           *metrics.Metrics
	dr          *drainrate.Estimator
	poisonTotal atomic.Int64
}

func New(t tenant.Tenant, q queue.Queue, tasks state.TaskStore, sm state.StateMachine, lim *limiter.Limiter, client *http.Client, b backoff.Backoff, d delayed.DelayedQueue, dlq dlq.DeadLetterQueue, m *metrics.Metrics, dr *drainrate.Estimator) *Worker {
	return &Worker{
		tenant:     t,
		queue:      q,
		tasks:      tasks,
		sm:         sm,
		limiter:    lim,
		client:     client,
		backoff:    b,
		delayed:    d,
		dlq:        dlq,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		leaseDur:   30 * time.Second,
		renewEvery: 10 * time.Second,
		m:          m,
		dr:         dr,
	}
}

// PoisonTotal 返回跳过的 poison message 数量（测试/指标用）。
func (w *Worker) PoisonTotal() int64 {
	return w.poisonTotal.Load()
}

// Run 循环投递直到 ctx 取消。
func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		w.deliverOnce(ctx)
	}
}

// deliverOnce 执行一次投递尝试；无任务/全局超时时短暂休眠后返回。
func (w *Worker) deliverOnce(ctx context.Context) {
	release, err := w.limiter.Acquire(ctx, w.tenant.ID)
	if err != nil {
		time.Sleep(10 * time.Millisecond)
		return
	}
	defer release()

	id, err := w.queue.Dequeue(ctx, w.tenant.ID)
	if errors.Is(err, queue.ErrEmpty) {
		time.Sleep(10 * time.Millisecond)
		return
	}
	if err != nil {
		log.Printf("[%s] dequeue error: %v", w.tenant.ID, err)
		time.Sleep(10 * time.Millisecond)
		return
	}

	task, err := w.tasks.Get(ctx, id)
	if errors.Is(err, state.ErrNotFound) {
		w.poisonTotal.Add(1)
		log.Printf("[%s] poison task %s skipped", w.tenant.ID, id)
		_ = w.queue.Ack(ctx, w.tenant.ID, id)
		return
	}
	if err != nil {
		log.Printf("[%s] get task %s error: %v", w.tenant.ID, id, err)
		_ = w.queue.Requeue(ctx, w.tenant.ID, id) // 避免 id 卡在 inflight（临时 Redis 错）
		return
	}

	token, err := w.sm.AcquireInFlight(ctx, id, w.leaseDur)
	if err != nil {
		log.Printf("[%s] acquire in-flight %s error: %v", w.tenant.ID, id, err)
		return
	}

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.heartbeat(hbCtx, id, token)

	status, retryAfter := w.deliver(ctx, task)
	if w.m != nil {
		w.m.DeliveryAttempts.WithLabelValues(w.tenant.ID, strconv.Itoa(status)).Inc()
	}
	if status >= 200 && status < 300 {
		if err := w.sm.Apply(ctx, id, state.Transition{From: state.StatusInFlight, To: state.StatusDelivered}, token); err != nil {
			w.stale()
			return // 旧 token：不 Ack、不计数，避免删除新 worker 的 inflight 项
		}
		if w.m != nil {
			w.m.DeliverySuccess.WithLabelValues(w.tenant.ID).Inc()
		}
		if w.dr != nil {
			w.dr.Observe(w.tenant.ID)
		}
		_ = w.queue.Ack(ctx, w.tenant.ID, id)
		return
	}

	delay := w.backoff.Next(task.Attempt, w.rng)
	if retryAfter > 0 {
		delay = retryAfter
	}
	if delay > w.backoff.Cap {
		delay = w.backoff.Cap
	}
	nextRetryAt := time.Now().Add(delay)
	deadline := task.IngestedAt.Add(w.tenant.RetryWindow)
	if nextRetryAt.After(deadline) {
		if err := w.sm.Apply(ctx, id, state.Transition{From: state.StatusInFlight, To: state.StatusDead}, token); err != nil {
			w.stale()
			return // 旧 token：不 dlq.Dead，避免移动新 worker 的任务
		}
		if w.m != nil {
			w.m.DeliveryPermanentFailure.WithLabelValues(w.tenant.ID).Inc()
		}
		_ = w.dlq.Dead(ctx, w.tenant.ID, id)
		return
	}
	if err := w.sm.ScheduleRetry(ctx, id, task.Attempt+1, nextRetryAt, token); err != nil {
		w.stale()
		log.Printf("[%s] schedule retry %s error: %v", w.tenant.ID, id, err)
		return
	}
	if w.m != nil {
		w.m.RetryDelay.WithLabelValues(w.tenant.ID).Observe(delay.Seconds())
	}
	_ = w.delayed.Schedule(ctx, w.tenant.ID, id, nextRetryAt)
	_ = w.queue.Ack(ctx, w.tenant.ID, id)
}

// stale 记录一次 fencing token 拒旧写（近似：所有状态写冲突）。
func (w *Worker) stale() {
	if w.m != nil {
		w.m.StaleWriteRejected.Inc()
	}
}

// heartbeat 独立续租；token 不再匹配（被 reaper 接管）时停止。
func (w *Worker) heartbeat(ctx context.Context, id state.TaskID, token uint64) {
	ticker := time.NewTicker(w.renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.sm.Renew(ctx, id, token, w.leaseDur); err != nil {
				if errors.Is(err, state.ErrConflict) {
					log.Printf("[%s] lease %s lost, stop heartbeat", w.tenant.ID, id)
					return
				}
				log.Printf("[%s] renew %s error (transient, retry): %v", w.tenant.ID, id, err)
			}
		}
	}
}

// deliver 把任务 POST 到下游，返回状态码与 Retry-After（秒；无则 0）。
func (w *Worker) deliver(ctx context.Context, task state.Task) (int, time.Duration) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.tenant.Destination, bytes.NewReader(task.Payload))
	if err != nil {
		return 0, 0
	}
	req.Header.Set("Content-Type", task.ContentType)
	req.Header.Set("Idempotency-Key", string(task.IdempotencyKey))
	resp, err := w.client.Do(req)
	if err != nil {
		log.Printf("[%s] deliver %s error: %v", w.tenant.ID, task.ID, err)
		return 0, 0
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	var ra time.Duration
	if s := resp.Header.Get("Retry-After"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			ra = time.Duration(n) * time.Second
		}
	}
	return resp.StatusCode, ra
}
