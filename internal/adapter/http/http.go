// Package http adapts the REST contract (docs/api.md) onto the ports.
// Chi routes; handlers depend only on port interfaces. Validation mirrors
// the Java DTO annotations field-for-field (see dto.go); the error envelope
// mirrors GlobalExceptionHandler, including the empty-body 401.
package http

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// RPSReader exposes the adaptive controller's current cap for GET /status.
// Implemented by internal/adaptive in EQLX-4; fakes/stubs until then.
type RPSReader interface {
	CurrentRPS() float64
}

// ThrottleRecorder receives completion samples for the adaptive
// controller. Optional (nil = unwired, tests); *adaptive.Controller
// implements it. Separate from RPSReader (read path) so the write path
// is explicit and fakes stay trivial: readers don't imply writers.
type ThrottleRecorder interface {
	RecordCompletion(durationMs int64, success bool)
}

// Deps wires a Handler. MaxPayloadBytes mirrors app.queue.max-payload-bytes
// (default 1048576); APIKey mirrors app.security.api-key.
type Deps struct {
	Tasks     port.TaskRepository
	Counts    port.CountsRepository
	Sequences port.SequenceStateRepository
	CMS       port.CMSStore
	Metrics   port.Metrics
	// MetricsPath + MetricsHandler expose the Prometheus registry OUTSIDE
	// /api/v1 auth (scrapers don't carry API keys). Both set or neither:
	// a path with no handler (or vice versa) is a wiring bug, fail loud
	// in NewRouter. Operators must not expose this path publicly.
	MetricsPath     string
	MetricsHandler  http.Handler
	RPS             RPSReader
	Throttle        ThrottleRecorder
	Clock           domain.Clock
	APIKey          string
	MaxPayloadBytes int
}

// Handler serves the /api/v1 contract.
type Handler struct {
	deps Deps
}

// NewRouter builds the chi router with auth on /api/v1/*.
func NewRouter(d Deps) http.Handler {
	if (d.MetricsPath == "") != (d.MetricsHandler == nil) {
		panic("http: MetricsPath and MetricsHandler must be set together")
	}
	h := &Handler{deps: d}
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(h.apiKey)
		r.Post("/tasks", h.createTask)
		r.Post("/tasks/{id}/complete", h.completeTask)
		r.Get("/tasks/{id}", h.getTask)
		r.Get("/tasks", h.listTasks)
		r.Get("/status", h.status)
	})
	if d.MetricsPath != "" {
		r.Handle(d.MetricsPath, d.MetricsHandler)
	}
	// Minimal liveness: 200 while the mux serves, outside auth (probes
	// carry no API keys). Stays 200 through shutdown drain by
	// construction (chi serves until Shutdown returns) — do NOT flip it
	// to 503 on drain: that is readiness leaking into liveness, and k8s
	// would restart a pod that is deliberately shutting down. Traffic
	// removal is /readyz's job (GAP-5). This is NOT readiness — DB,
	// locks, and migrations are not checked here.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return r
}

// apiKey enforces X-API-Key. Missing/wrong → 401 with an EMPTY body
// (Java parity: sendError(401), not the error envelope).
func (h *Handler) apiKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(h.deps.APIKey)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type envelope struct {
	Code        string       `json:"code"`
	Message     string       `json:"message"`
	Timestamp   time.Time    `json:"timestamp"`
	FieldErrors []fieldError `json:"fieldErrors"`
}

func (h *Handler) now() time.Time {
	if h.deps.Clock != nil {
		return h.deps.Clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) fail(w http.ResponseWriter, status int, code, message string, fields []fieldError) {
	h.writeJSON(w, status, envelope{
		Code: code, Message: message, Timestamp: h.now(), FieldErrors: fields,
	})
}

func (h *Handler) badRequest(w http.ResponseWriter, message string) {
	h.fail(w, http.StatusBadRequest, "BAD_REQUEST", message, nil)
}

func (h *Handler) validationFailed(w http.ResponseWriter, fields []fieldError) {
	h.fail(w, http.StatusBadRequest, "VALIDATION_FAILED", "Request validation failed", fields)
}

func (h *Handler) notFound(w http.ResponseWriter, message string) {
	h.fail(w, http.StatusNotFound, "NOT_FOUND", message, nil)
}

func (h *Handler) internal(w http.ResponseWriter) {
	h.fail(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error", nil)
}

// Auth is config-only by Java parity: a single static key
// (app.security.api-key), no per-tenant keys, no DB table, no rotation.
// The middleware validates against the startup-loaded key, so no port is
// needed and no tenant flows from the key — the fairness key always comes
// from the request body (or Kafka record key), never from auth. Implication,
// stated once: the scheduler is NOT multi-tenant at the auth layer — one
// key unlocks everything — and only at the fairness layer. Do not add
// per-tenant keys without a spec section and a port.
func isNotFound(err error) bool { return errors.Is(err, port.ErrNotFound) }

func isConflict(err error) bool { return errors.Is(err, port.ErrVersionConflict) }
