//go:build differential

package differential

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// ProcSpec describes one supervised child (Java or Go service). Startup is
// bounded: a JVM with a bad classpath or a Go binary on a squatted port
// fails fast naming the culprit — never hangs the harness.
type ProcSpec struct {
	Name string
	Bin  string
	Args []string
	Env  map[string]string
	// ReadyURL polled until 2xx/4xx (any HTTP response = alive).
	ReadyURL string
	// StartupTimeout bounds readiness. Zero means 60s.
	StartupTimeout time.Duration
}

// Proc is a supervised child process. SpawnedAt/ReadyAt feed the
// results.json startup section: T0 = spawn, T1 = first HTTP response on
// the readiness URL (any status — even 401 — counts as alive; the gate is
// "socket accepting and routing", readiness semantics stay per-side).
type Proc struct {
	spec ProcSpec
	cmd  *exec.Cmd
	SpawnedAt time.Time
	ReadyAt   time.Time
}

// Launch starts the process and waits for readiness. Failure names the
// side ("java failed to become ready in 15s: ..."), so a stuck JVM never
// reads as a slow comparison.
func Launch(ctx context.Context, spec ProcSpec) (*Proc, error) {
	if spec.StartupTimeout <= 0 {
		spec.StartupTimeout = 60 * time.Second
	}
	cmd := exec.Command(spec.Bin, spec.Args...)
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	spawned := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("differential: start %s: %w", spec.Name, err)
	}
	p := &Proc{spec: spec, cmd: cmd, SpawnedAt: spawned}
	deadline := time.Now().Add(spec.StartupTimeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, "GET", spec.ReadyURL, nil)
		if err != nil {
			p.kill()
			return nil, fmt.Errorf("differential: %s readiness request: %w", spec.Name, err)
		}
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			p.ReadyAt = time.Now()
			return p, nil // any HTTP response = alive (even 401/404)
		}
		if time.Now().After(deadline) {
			p.kill()
			return nil, fmt.Errorf("differential: %s not ready in %v (bad classpath? blocked port? crashed?)",
				spec.Name, spec.StartupTimeout)
		}
		select {
		case <-ctx.Done():
			p.kill()
			return nil, fmt.Errorf("differential: %s startup cancelled: %w", spec.Name, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// PID reports the child PID for the results artifact.
func (p *Proc) PID() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// Stop SIGTERMs, waits 10s, then SIGKILLs. A process that dies from our
// own SIGTERM reports clean (nil); anything else (non-zero exit on its
// own, SIGKILL after the grace) is an error. Always reaps the process.
func (p *Proc) Stop() error {
	if p.cmd.Process == nil {
		return nil
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
		return fmt.Errorf("differential: %s ignored SIGTERM, killed", p.spec.Name)
	case err := <-done:
		if err != nil && !killedBySignal(err, syscall.SIGTERM) {
			return fmt.Errorf("differential: %s exited: %w", p.spec.Name, err)
		}
		return nil
	}
}

func killedBySignal(err error, sig syscall.Signal) bool {
	exit, ok := err.(*exec.ExitError)
	if !ok {
		return false
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	return status.Signaled() && status.Signal() == sig
}

func (p *Proc) kill() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		_, _ = p.cmd.Process.Wait()
	}
}
