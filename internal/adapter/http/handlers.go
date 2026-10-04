package http

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/synanton/equalix-go/internal/domain"
)

// createTaskRequest mirrors Java CreateTaskRequest (field rules quoted):
// fairnessKey @NotBlank; weight @Positive default 1.0; payload @NotNull
// (base64); sequential default false; sequenceNumber required iff
// sequential; dependsOnTaskId/previous-result optional.
type createTaskRequest struct {
	FairnessKey            string   `json:"fairnessKey"`
	Weight                 *float64 `json:"weight"`
	Payload                *string  `json:"payload"`
	Sequential             bool     `json:"sequential"`
	SequenceNumber         *int64   `json:"sequenceNumber"`
	DependsOnTaskID        *string  `json:"dependsOnTaskId"`
	RequiresPreviousResult bool     `json:"requiresPreviousResult"`
}

// completeTaskRequest mirrors Java CompleteTaskRequest: success default
// false; error required non-blank iff success is false.
type completeTaskRequest struct {
	Success bool    `json:"success"`
	Result  *string `json:"result"`
	Error   *string `json:"error"`
}

// taskStatusResponse mirrors Java TaskStatusResponse. Nullable fields are
// pointers so the wire carries explicit nulls (priority null until QUEUED,
// completedAt/lastError null until terminal/failure).
type taskStatusResponse struct {
	ID          string     `json:"id"`
	FairnessKey string     `json:"fairnessKey"`
	Status      string     `json:"status"`
	Priority    *int64     `json:"priority"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt"`
	RetryCount  int        `json:"retryCount"`
	LastError   *string    `json:"lastError"`
}

// systemStatusResponse mirrors Java SystemStatusResponse.
type systemStatusResponse struct {
	InFlight   int64   `json:"inFlight"`
	CurrentRPS float64 `json:"currentRps"`
}

func toStatusResponse(t *domain.Task) taskStatusResponse {
	out := taskStatusResponse{
		ID: t.ID, FairnessKey: t.FairnessKey, Status: string(t.Status),
		CreatedAt: t.CreatedAt.UTC(), RetryCount: t.RetryCount,
	}
	if t.HasPriority {
		p := t.Priority
		out.Priority = &p
	}
	if !t.CompletedAt.IsZero() {
		c := t.CompletedAt.UTC()
		out.CompletedAt = &c
	}
	// retryCount is present-but-inert (DECISION-2): the Go scheduler never
	// mutates it, so EQLX-5 differential tests must encode "expected
	// divergence" against any Java-mutated value rather than fail on mismatch.
	if t.LastError != "" {
		e := t.LastError
		out.LastError = &e
	}
	return out
}

// createTask ingests one task → 201 with the new UUID (JSON string).
func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.badRequest(w, "malformed JSON body")
		return
	}
	var fields []fieldError
	if req.FairnessKey == "" {
		fields = append(fields, fieldError{"fairnessKey", "must not be blank"})
	}
	weight := 1.0
	if req.Weight != nil {
		if *req.Weight <= 0 {
			fields = append(fields, fieldError{"weight", "must be greater than 0"})
		} else {
			weight = *req.Weight
		}
	}
	var payload []byte
	if req.Payload == nil {
		fields = append(fields, fieldError{"payload", "must not be null"})
	} else {
		var err error
		payload, err = base64.StdEncoding.DecodeString(*req.Payload)
		if err != nil {
			fields = append(fields, fieldError{"payload", "must be base64"})
		}
	}
	if req.Sequential && req.SequenceNumber == nil {
		fields = append(fields, fieldError{"sequenceNumber", "sequenceNumber is required when sequential is true"})
	}
	if len(fields) > 0 {
		h.validationFailed(w, fields)
		return
	}
	// Size cap is a use-case rule (CreateTaskUseCase → IAE), hence
	// BAD_REQUEST rather than VALIDATION_FAILED (Java parity).
	if len(payload) > h.deps.MaxPayloadBytes {
		h.badRequest(w, fmt.Sprintf("payload exceeds max size of %d bytes", h.deps.MaxPayloadBytes))
		return
	}
	now := h.now()
	task := &domain.Task{
		ID: uuid.NewString(), FairnessKey: req.FairnessKey, Weight: weight,
		Status: domain.StatusReceived, CreatedAt: now, UpdatedAt: now,
		Sequential: req.Sequential, RequiresPreviousResult: req.RequiresPreviousResult,
	}
	if req.SequenceNumber != nil {
		task.SequenceNumber = *req.SequenceNumber
	}
	if req.DependsOnTaskID != nil {
		task.DependsOnTaskID = *req.DependsOnTaskID
	}
	if task.Sequential {
		// First task of a key must find dispatchable state (Java parity:
		// ingestion findOrCreates the sequence row).
		if _, err := h.deps.Sequences.FindOrCreate(r.Context(), task.FairnessKey); err != nil {
			h.internal(w)
			return
		}
	}
	if err := h.deps.Tasks.Save(r.Context(), task); err != nil {
		h.internal(w)
		return
	}
	h.writeJSON(w, http.StatusCreated, task.ID)
}

// completeTask implements the B2 protocol: terminal → no-write success;
// non-in-flight → 400; in-flight → terminal Save, then exactly one
// Decrement + CMS -1 + metrics record. Version conflict → re-read.
//
// Transactionality (spec §13 NOTE): two distinct windows. (1) Save →
// Decrement is DB-only and closable via Transact — deferred to EQLX-3,
// which will own this path; until then a crash here leaves an
// over-reported slot for the watchdog. (2) DB commit → CMS flush is
// cross-store and fundamentally unclosable (§8 drift by design).
func (h *Handler) completeTask(w http.ResponseWriter, r *http.Request) {
	var req completeTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.badRequest(w, "malformed JSON body")
		return
	}
	if !req.Success && (req.Error == nil || *req.Error == "") {
		h.validationFailed(w, []fieldError{{"error", "error must be provided when success is false"}})
		return
	}
	var result []byte
	if req.Result != nil {
		var err error
		result, err = base64.StdEncoding.DecodeString(*req.Result)
		if err != nil {
			h.validationFailed(w, []fieldError{{"result", "must be base64"}})
			return
		}
	}
	_ = result // stored with the result path (ingestion/completion result writes extend the port; see TODO(payload))
	id := chi.URLParam(r, "id")
	task, err := h.deps.Tasks.FindByID(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			h.notFound(w, "Task not found: "+id)
			return
		}
		h.internal(w)
		return
	}
	if task.Status.IsTerminal() {
		w.WriteHeader(http.StatusOK) // duplicate delivery: ignored (spec §5.3)
		return
	}
	if !task.Status.IsInFlight() {
		h.badRequest(w, "task is not in flight: "+id)
		return
	}
	now := h.now()
	durationMs := now.Sub(task.UpdatedAt).Milliseconds()
	if req.Success {
		task.Status = domain.StatusSucceeded
	} else {
		task.Status = domain.StatusFailed
		task.LastError = *req.Error
	}
	task.CompletedAt = now
	if err := h.deps.Tasks.Save(r.Context(), task); err != nil {
		if isConflict(err) {
			re, rerr := h.deps.Tasks.FindByID(r.Context(), id)
			if rerr == nil && re.Status.IsTerminal() {
				w.WriteHeader(http.StatusOK) // lost race to a duplicate: success
				return
			}
		}
		h.badRequest(w, "concurrent completion for task: "+id)
		return
	}
	if task.Sequential {
		h.advanceSequence(r, task, req.Success, now, w)
		return
	}
	h.releaseSlot(r, task, req.Success, durationMs, w)
}

// advanceSequence applies the sequential cursor transitions (spec §6.4).
// Immediate next-sequence dispatch belongs to the jobs layer (EQLX-3);
// until then the key's cursor is correct and its next task waits QUEUED.
func (h *Handler) advanceSequence(r *http.Request, task *domain.Task, success bool, now time.Time, w http.ResponseWriter) {
	ctx := r.Context()
	st, err := h.deps.Sequences.FindOrCreate(ctx, task.FairnessKey)
	if err != nil {
		h.internal(w)
		return
	}
	if success {
		st.OnSuccess(task.SequenceNumber)
	} else {
		st.OnFailure(now)
	}
	if err := h.deps.Sequences.Save(ctx, st); err != nil {
		h.internal(w)
		return
	}
	h.releaseSlot(r, task, success, now.Sub(task.UpdatedAt).Milliseconds(), w)
}

func (h *Handler) releaseSlot(r *http.Request, task *domain.Task, success bool, durationMs int64, w http.ResponseWriter) {
	ctx := r.Context()
	if err := h.deps.Counts.Decrement(ctx, task.FairnessKey); err != nil {
		h.internal(w)
		return
	}
	if err := h.deps.CMS.Add(ctx, task.FairnessKey, -1); err != nil {
		h.internal(w)
		return
	}
	result := "failed"
	if success {
		result = "success"
	}
	h.deps.Metrics.RecordCompletion(task.FairnessKey, result, durationMs)
	// Controller sample: same durationMs the metrics path uses — the §4
	// UpdatedAt-derived latency, not a fresh stamp.
	if h.deps.Throttle != nil {
		h.deps.Throttle.RecordCompletion(durationMs, success)
	}
	w.WriteHeader(http.StatusOK)
}

// getTask returns one task's status or 404.
func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	task, err := h.deps.Tasks.FindByID(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			h.notFound(w, "Task not found: "+id)
			return
		}
		h.internal(w)
		return
	}
	h.writeJSON(w, http.StatusOK, toStatusResponse(task))
}

// listTasks returns a key's tasks, optional single-status filter.
func (h *Handler) listTasks(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("fairnessKey")
	if key == "" {
		h.badRequest(w, "missing required query parameter: fairnessKey")
		return
	}
	var status *domain.Status
	if raw := r.URL.Query().Get("status"); raw != "" {
		s := domain.Status(raw)
		switch s {
		case domain.StatusReceived, domain.StatusQueued, domain.StatusDispatched,
			domain.StatusCommitted, domain.StatusSucceeded, domain.StatusFailed,
			domain.StatusTimeout:
			status = &s
		default:
			h.badRequest(w, "invalid status: "+raw)
			return
		}
	}
	tasks, err := h.deps.Tasks.ListByKey(r.Context(), key, status)
	if err != nil {
		h.internal(w)
		return
	}
	out := make([]taskStatusResponse, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, toStatusResponse(t))
	}
	h.writeJSON(w, http.StatusOK, out)
}

// status returns the CMS total estimate and the RPS cap.
func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	total, err := h.deps.CMS.Total(r.Context())
	if err != nil {
		h.internal(w)
		return
	}
	h.writeJSON(w, http.StatusOK, systemStatusResponse{InFlight: total, CurrentRPS: h.deps.RPS.CurrentRPS()})
}
