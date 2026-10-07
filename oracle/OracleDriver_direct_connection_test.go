/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	oracleTest "github.com/oracle/go-oracledb/v26/internal/tests"
)

// directConnectionCycle is one physical connection from establishment through
// query and closure. Its counters describe client actions, not server sessions.
type directConnectionCycle struct {
	ConnectorCreated bool
	Connected        bool
	Queried          bool
	RowsClosed       bool
	Closed           bool
}

type lifecycleConnectorFactory func() (driver.Connector, error)
type lifecycleEvent func(string)
type lifecycleSessionInitializer func(context.Context, driver.Conn, lifecycleEvent) error

func (event lifecycleEvent) emit(stage string) {
	if event != nil {
		event(stage)
	}
}

// Cleanup has no context API. Convert a close panic to a diagnostic error so
// row-close failure cannot prevent the owning connection from being closed.
func lifecycleClose(stage string, closeResource func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%s panic: %v\n%s", stage, p, debug.Stack())
		}
	}()
	if err = closeResource(); err != nil {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return nil
}

// executeDirectConnectionCycle is shared by the functional baseline and the
// concurrent stress workload. Each invocation constructs a connector, connects
// once, and owns all cleanup. Defers finish before the next cycle begins.
func executeDirectConnectionCycle(ctx context.Context, factory lifecycleConnectorFactory, initialize lifecycleSessionInitializer, event lifecycleEvent) (result directConnectionCycle, err error) {
	event.emit("cycles_started")
	stage := "create connector"
	defer func() {
		if p := recover(); p != nil {
			err = errors.Join(err, fmt.Errorf("%s panic: %v\n%s", stage, p, debug.Stack()))
		}
		if err == nil && ctx.Err() != nil {
			err = fmt.Errorf("operation deadline: %w", ctx.Err())
		}
		if err != nil {
			event.emit("failed_cycles")
		} else {
			event.emit("completed_cycles")
		}
	}()
	connector, createErr := factory()
	if createErr != nil {
		return result, fmt.Errorf("create connector: %w", createErr)
	}
	if connector == nil {
		return result, errors.New("create connector: returned nil without an error")
	}
	result.ConnectorCreated = true
	event.emit("connectors_created")
	event.emit("connection_attempts")
	stage = "connect"
	conn, connectErr := connector.Connect(ctx)
	// Also close a non-nil partial connection returned with an error.
	if conn != nil {
		defer func() {
			if closeErr := lifecycleClose("connection close", conn.Close); closeErr != nil {
				err = errors.Join(err, closeErr)
			} else {
				result.Closed = true
				event.emit("connection_closes")
			}
		}()
	}
	if connectErr != nil {
		return result, fmt.Errorf("connect: %w", connectErr)
	}
	if conn == nil {
		return result, errors.New("connect: returned a nil connection without an error")
	}
	result.Connected = true
	event.emit("successful_connections")
	// Initialization happens under the same owner, so opened connections remain
	// counted and are closed even if protocol verification or tagging fails.
	if initialize != nil {
		stage = "session initialization"
		if initErr := initialize(ctx, conn, event); initErr != nil {
			return result, initErr
		}
	}

	stage = "query"
	queryer, ok := conn.(driver.QueryerContext)
	if !ok {
		return result, errors.New("query: connection does not implement driver.QueryerContext")
	}
	rows, queryErr := queryer.QueryContext(ctx, "SELECT 1 FROM DUAL", nil)
	if rows != nil {
		defer func() {
			if closeErr := lifecycleClose("rows close", rows.Close); closeErr != nil {
				err = errors.Join(err, closeErr)
			} else {
				result.RowsClosed = true
				event.emit("row_closes")
			}
		}()
	}
	if queryErr != nil {
		return result, fmt.Errorf("query: %w", queryErr)
	}
	if rows == nil {
		return result, errors.New("query: returned nil rows without an error")
	}

	columns := rows.Columns()
	if len(columns) != 1 {
		return result, fmt.Errorf("query: DUAL returned %d columns, want 1", len(columns))
	}
	values := make([]driver.Value, 1)
	if nextErr := rows.Next(values); nextErr != nil {
		if errors.Is(nextErr, io.EOF) {
			return result, errors.New("query: DUAL returned no rows")
		}
		return result, fmt.Errorf("query: iterate DUAL: %w", nextErr)
	}
	if fmt.Sprint(values[0]) != "1" {
		return result, fmt.Errorf("query: DUAL returned %v, want 1", values[0])
	}
	if nextErr := rows.Next(values); nextErr == nil {
		return result, errors.New("query: DUAL returned more than one row")
	} else if !errors.Is(nextErr, io.EOF) {
		return result, fmt.Errorf("query: iterate DUAL: %w", nextErr)
	}
	result.Queried = true
	event.emit("query_completions")
	return result, nil
}

const lifecycleModule = "go-connection-lifecycle-stress"

func newLifecycleRunID() string { return "cnx-" + strings.TrimPrefix(newRetentionRunID(), "ret-") }

func initializeLifecycleSession(runID string) lifecycleSessionInitializer {
	return func(ctx context.Context, conn driver.Conn, event lifecycleEvent) error {
		if err := verifyLifecycleTCPS(ctx, conn); err != nil {
			return err
		}
		event.emit("tcps_verifications")
		execer, ok := conn.(driver.ExecerContext)
		if !ok {
			return errors.New("session tagging: connection lacks driver.ExecerContext")
		}
		_, err := execer.ExecContext(ctx, `BEGIN DBMS_SESSION.SET_IDENTIFIER(:run_id); DBMS_APPLICATION_INFO.SET_MODULE(:module, 'workload'); END;`, []driver.NamedValue{
			{Name: "run_id", Ordinal: 1, Value: runID}, {Name: "module", Ordinal: 2, Value: lifecycleModule},
		})
		if err != nil {
			return fmt.Errorf("session tagging: %w", err)
		}
		event.emit("session_identifications")
		return nil
	}
}

func verifyLifecycleTCPS(ctx context.Context, conn driver.Conn) (err error) {
	queryer, ok := conn.(driver.QueryerContext)
	if !ok {
		return errors.New("protocol verification: connection lacks driver.QueryerContext")
	}
	rows, err := queryer.QueryContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') FROM DUAL", nil)
	if rows != nil {
		defer func() { err = errors.Join(err, lifecycleClose("protocol verification rows close", rows.Close)) }()
	}
	if err != nil {
		return fmt.Errorf("protocol verification: %w", err)
	}
	if rows == nil {
		return errors.New("protocol verification: returned nil rows")
	}
	values := make([]driver.Value, 1)
	if len(rows.Columns()) != 1 {
		return errors.New("protocol verification: expected one column")
	}
	if err = rows.Next(values); err != nil {
		return fmt.Errorf("protocol verification: %w", err)
	}
	if !strings.EqualFold(fmt.Sprint(values[0]), "tcps") {
		return errors.New("protocol verification: server connection is not TCPS")
	}
	if err = rows.Next(values); err != io.EOF {
		if err == nil {
			return errors.New("protocol verification: returned extra rows")
		}
		return fmt.Errorf("protocol verification: %w", err)
	}
	return nil
}

// TestDriver_DirectConnectionLifecycle proves the exact operation that the
// stress test repeats, before concurrency or volume complicate a failure.
func TestDriver_DirectConnectionLifecycle(t *testing.T) {
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	if !strings.EqualFold(strings.TrimSpace(TestingConfig.Database.Protocol), "tcps") {
		t.Skip("direct TCPS lifecycle baseline requires a selected TCPS profile; no TCPS coverage ran")
	}
	if err := validateRetentionTLS(TestingConfig); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := executeDirectConnectionCycle(ctx, func() (driver.Connector, error) { return openTestConnectorWithConfig(TestingConfig) }, initializeLifecycleSession(newLifecycleRunID()), nil)
	if err != nil {
		t.Fatal(lifecycleSafeError(err, TestingConfig))
	}
	if !result.ConnectorCreated || !result.Connected || !result.Queried || !result.RowsClosed || !result.Closed {
		t.Fatalf("incomplete direct connection lifecycle: %+v", result)
	}
}

func TestDirectConnectionCycleUnit(t *testing.T) {
	connectError := errors.New("connect failed")
	queryError := errors.New("query failed")
	iterateError := errors.New("iterate failed")
	rowsCloseError := errors.New("rows close failed")
	connCloseError := errors.New("connection close failed")
	cases := []struct {
		name       string
		configure  func(*retentionFakeConnector)
		wantError  string
		wantClosed int64
		wantRows   int64
	}{
		{name: "success", wantClosed: 1, wantRows: 1},
		{name: "connect error", configure: func(c *retentionFakeConnector) {
			c.connect = func(context.Context, int64) error { return connectError }
		}, wantError: "connect", wantClosed: 0},
		{name: "query error", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) { return nil, queryError }
		}, wantError: "query", wantClosed: 1},
		{name: "no rows", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) {
				return &retentionFakeRows{closed: &c.rowsClosed}, nil
			}
		}, wantError: "no rows", wantClosed: 1, wantRows: 1},
		{name: "incorrect value", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) {
				return &retentionFakeRows{values: []driver.Value{int64(2)}, closed: &c.rowsClosed}, nil
			}
		}, wantError: "want 1", wantClosed: 1, wantRows: 1},
		{name: "extra row", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) {
				return &retentionFakeRows{values: []driver.Value{int64(1), int64(1)}, closed: &c.rowsClosed}, nil
			}
		}, wantError: "more than one", wantClosed: 1, wantRows: 1},
		{name: "iteration error", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) {
				return &retentionFakeRows{values: []driver.Value{int64(1)}, nextError: iterateError, closed: &c.rowsClosed}, nil
			}
		}, wantError: "iterate DUAL", wantClosed: 1, wantRows: 1},
		{name: "rows close error", configure: func(c *retentionFakeConnector) {
			c.query = func(context.Context, int64) (driver.Rows, error) {
				return &retentionFakeRows{values: []driver.Value{int64(1)}, closeError: rowsCloseError, closed: &c.rowsClosed}, nil
			}
		}, wantError: "rows close", wantClosed: 1, wantRows: 1},
		{name: "connection close error", configure: func(c *retentionFakeConnector) {
			c.closeError = connCloseError
		}, wantError: "connection close", wantClosed: 1, wantRows: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			connector := &retentionFakeConnector{}
			if tc.configure != nil {
				tc.configure(connector)
			}
			result, err := executeDirectConnectionCycle(context.Background(), func() (driver.Connector, error) { return connector, nil }, nil, nil)
			if tc.wantError == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("got error %v, want %q", err, tc.wantError)
			}
			if got := connector.closed.Load(); got != tc.wantClosed {
				t.Errorf("connection close calls = %d, want %d", got, tc.wantClosed)
			}
			if got := connector.rowsClosed.Load(); got != tc.wantRows {
				t.Errorf("rows close calls = %d, want %d", got, tc.wantRows)
			}
			if tc.wantError == "" && (!result.Connected || !result.Queried || !result.Closed) {
				t.Errorf("incomplete successful lifecycle: %+v", result)
			}
		})
	}
}

func lifecycleUnitConfig() oracleTest.ConnectionLifecycleConfig {
	return oracleTest.ConnectionLifecycleConfig{Workers: 4, CyclesPerWorker: 5, Timeout: "5s"}
}

func lifecycleUnitInitializer(_ context.Context, _ driver.Conn, event lifecycleEvent) error {
	event.emit("tcps_verifications")
	event.emit("session_identifications")
	return nil
}

type lifecyclePanicCloseRows struct{ driver.Rows }

func (r lifecyclePanicCloseRows) Close() error {
	_ = r.Rows.Close()
	panic("injected row close panic")
}

func TestConnectionLifecycleWorkersUnit(t *testing.T) {
	for _, kind := range []string{"nil connector", "nil connection", "partial connection", "partial connection close failure", "nil rows", "query error with rows"} {
		t.Run(kind, func(t *testing.T) {
			primary, cleanup := errors.New("primary failure"), errors.New("cleanup failure")
			c := &retentionFakeConnector{}
			want := lifecycleCounts{CyclesStarted: 1, ConnectorsCreated: 1, ConnectionAttempts: 1, SuccessfulConnections: 1, ConnectionCloses: 1, FailedCycles: 1}
			wantClose, wantRows, wantStage := int64(1), int64(0), "query"
			factory := lifecycleConnectorFactory(func() (driver.Connector, error) { return c, nil })
			switch kind {
			case "nil connector":
				factory = func() (driver.Connector, error) { return nil, nil }
				want = lifecycleCounts{CyclesStarted: 1, FailedCycles: 1}
				wantClose, wantStage = 0, "create connector"
			case "nil connection":
				factory = func() (driver.Connector, error) { return lifecycleSessionConnector{}, nil }
				want.SuccessfulConnections, want.ConnectionCloses = 0, 0
				wantClose, wantStage = 0, "connect"
			case "partial connection", "partial connection close failure":
				factory = func() (driver.Connector, error) {
					return lifecycleSessionConnector{conn: &retentionFakeConn{connector: c}, err: primary}, nil
				}
				want.SuccessfulConnections = 0
				wantStage = "connect"
				if kind == "partial connection close failure" {
					c.closeError = cleanup
					want.ConnectionCloses = 0
				}
			case "nil rows":
				c.query = func(context.Context, int64) (driver.Rows, error) { return nil, nil }
			case "query error with rows":
				c.query = func(context.Context, int64) (driver.Rows, error) {
					return &retentionFakeRows{closed: &c.rowsClosed}, primary
				}
				want.RowCloses, wantRows = 1, 1
			}
			p := &lifecycleProgress{}
			_, err := executeDirectConnectionCycle(context.Background(), factory, nil, p.record)
			if err == nil || !strings.Contains(err.Error(), wantStage) {
				t.Fatalf("missing stage failure: %v", err)
			}
			if (strings.HasPrefix(kind, "partial connection") || kind == "query error with rows") && !errors.Is(err, primary) {
				t.Fatalf("primary error lost: %v", err)
			}
			if kind == "partial connection close failure" && !errors.Is(err, cleanup) {
				t.Fatalf("cleanup error lost: %v", err)
			}
			if got := p.snapshot(); got != want || c.closed.Load() != wantClose || c.rowsClosed.Load() != wantRows {
				t.Fatalf("counts=%+v want=%+v close_calls=%d rows_close_calls=%d", got, want, c.closed.Load(), c.rowsClosed.Load())
			}
		})
	}
	t.Run("close panics preserve primary error and attempt both cleanups", func(t *testing.T) {
		cause := errors.New("injected query failure")
		c := &retentionFakeConnector{}
		c.query = func(context.Context, int64) (driver.Rows, error) {
			return lifecyclePanicCloseRows{&retentionFakeRows{closed: &c.rowsClosed}}, cause
		}
		c.onClose = func() { panic("injected connection close panic") }
		p := &lifecycleProgress{}
		_, err := executeDirectConnectionCycle(context.Background(), func() (driver.Connector, error) { return c, nil }, nil, p.record)
		if !errors.Is(err, cause) || !strings.Contains(err.Error(), "rows close panic") || !strings.Contains(err.Error(), "connection close panic") {
			t.Fatalf("lost primary or cleanup failure: %v", err)
		}
		counts := p.snapshot()
		if c.closed.Load() != 1 || c.rowsClosed.Load() != 1 || counts.FailedCycles != 1 || counts.ConnectionCloses != 0 || counts.RowCloses != 0 {
			t.Fatalf("incorrect cleanup accounting: %+v", counts)
		}
	})
	t.Run("fresh connector for each cycle", func(t *testing.T) {
		var mu sync.Mutex
		var connectors []*retentionFakeConnector
		factory := func() (driver.Connector, error) {
			c := &retentionFakeConnector{}
			mu.Lock()
			connectors = append(connectors, c)
			mu.Unlock()
			return c, nil
		}
		result, err := runConnectionLifecycleWorkers(context.Background(), lifecycleUnitConfig(), factory, lifecycleUnitInitializer, &lifecycleProgress{})
		if err != nil {
			t.Fatal(err)
		}
		if result.Counts.CompletedCycles != 20 || len(connectors) != 20 {
			t.Fatalf("counts=%+v connectors=%d", result.Counts, len(connectors))
		}
		for _, c := range connectors {
			if c.opened.Load() != 1 || c.closed.Load() != 1 || c.queries.Load() != 1 || c.rowsClosed.Load() != 1 {
				t.Fatalf("connector reused or not closed: opened=%d closed=%d", c.opened.Load(), c.closed.Load())
			}
		}
	})
	for _, stage := range []string{"create connector", "connect", "initialize", "query", "connection close", "panic"} {
		t.Run(stage, func(t *testing.T) {
			cause := errors.New("injected " + stage)
			factory := func() (driver.Connector, error) {
				if stage == "create connector" {
					return nil, cause
				}
				if stage == "panic" {
					panic("injected factory panic")
				}
				c := &retentionFakeConnector{}
				if stage == "connect" {
					c.connect = func(context.Context, int64) error { return cause }
				}
				if stage == "query" {
					c.query = func(context.Context, int64) (driver.Rows, error) { return nil, cause }
				}
				if stage == "connection close" {
					c.closeError = cause
				}
				return c, nil
			}
			init := lifecycleUnitInitializer
			if stage == "initialize" {
				init = func(context.Context, driver.Conn, lifecycleEvent) error { return cause }
			}
			result, err := runConnectionLifecycleWorkers(context.Background(), lifecycleUnitConfig(), factory, init, &lifecycleProgress{})
			if err == nil || !strings.Contains(err.Error(), "worker ") || !strings.Contains(err.Error(), stage) {
				t.Fatalf("got %v, want worker/stage error", err)
			}
			if stage != "panic" && !errors.Is(err, cause) {
				t.Fatalf("cause was lost: %v", err)
			}
			if result.Counts.WorkersFinished != 4 || result.Counts.FailedCycles == 0 {
				t.Fatalf("workers not joined: %+v", result.Counts)
			}
			if stage == "initialize" || stage == "query" {
				if result.Counts.SuccessfulConnections != result.Counts.ConnectionCloses {
					t.Fatalf("opened connections lost from counts: %+v", result.Counts)
				}
			}
		})
	}
	t.Run("duration drains healthy cycle", func(t *testing.T) {
		config := lifecycleUnitConfig()
		config.Workers = 1
		config.CyclesPerWorker = 0
		config.Duration = "30ms"
		config.Timeout = "20m"
		started, release := make(chan struct{}), make(chan struct{})
		c := &retentionFakeConnector{}
		c.query = func(ctx context.Context, _ int64) (driver.Rows, error) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &retentionFakeRows{values: []driver.Value{int64(1)}, closed: &c.rowsClosed}, nil
		}
		var result connectionLifecycleRunResult
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			result, err = runConnectionLifecycleWorkers(context.Background(), config, func() (driver.Connector, error) { return c, nil }, lifecycleUnitInitializer, &lifecycleProgress{})
		}()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("cycle did not start")
		}
		time.Sleep(60 * time.Millisecond)
		close(release)
		<-done
		if err != nil || result.Counts.CompletedCycles != 1 || result.Drain <= 0 {
			t.Fatalf("duration cancelled healthy cycle: result=%+v err=%v", result, err)
		}
	})
	for _, kind := range []string{"operation", "overall"} {
		t.Run(kind+" deadline", func(t *testing.T) {
			config := lifecycleUnitConfig()
			config.Workers = 1
			config.OperationTimeout = "10ms"
			ctx := context.Background()
			if kind == "overall" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
				config.OperationTimeout = "2s"
			}
			c := &retentionFakeConnector{connect: func(ctx context.Context, _ int64) error { <-ctx.Done(); return ctx.Err() }}
			result, err := runConnectionLifecycleWorkers(ctx, config, func() (driver.Connector, error) { return c, nil }, nil, &lifecycleProgress{})
			if !errors.Is(err, context.DeadlineExceeded) || result.Counts.WorkersFinished != 1 {
				t.Fatalf("got %+v %v", result, err)
			}
		})
	}
}

// A protocol query and a representative query have different row ownership.
// This fake also checks the run identifier and module supplied to Oracle.
type lifecycleSessionFake struct {
	*retentionFakeConn
	protocol                                      []driver.Value
	protocolError, errorOnProtocolClose, tagError error
	tagged                                        bool
}

func (c *lifecycleSessionFake) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "NETWORK_PROTOCOL") {
		if c.protocolError != nil {
			return nil, c.protocolError
		}
		return &retentionFakeRows{values: c.protocol, closeError: c.errorOnProtocolClose, closed: &c.connector.rowsClosed}, nil
	}
	return c.retentionFakeConn.QueryContext(ctx, query, args)
}
func (c *lifecycleSessionFake) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(query, "SET_IDENTIFIER") || len(args) != 2 || args[0].Value != "cnx-unit" || args[1].Value != lifecycleModule {
		return nil, errors.New("bad session identification")
	}
	c.tagged = true
	return driver.RowsAffected(0), c.tagError
}

type lifecycleSessionConnector struct {
	conn driver.Conn
	err  error
}

func (c lifecycleSessionConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, c.err
}
func (lifecycleSessionConnector) Driver() driver.Driver { return retentionFakeDriver{} }

func TestConnectionLifecycleSessionUnit(t *testing.T) {
	for _, kind := range []string{"success", "tcp", "empty", "extra", "protocol query", "protocol close", "tag"} {
		t.Run(kind, func(t *testing.T) {
			c := &retentionFakeConnector{}
			conn := &lifecycleSessionFake{retentionFakeConn: &retentionFakeConn{connector: c}, protocol: []driver.Value{"tcps"}}
			switch kind {
			case "tcp":
				conn.protocol = []driver.Value{"tcp"}
			case "empty":
				conn.protocol = nil
			case "extra":
				conn.protocol = []driver.Value{"tcps", "tcps"}
			case "protocol query":
				conn.protocolError = errors.New("protocol query failed")
			case "protocol close":
				conn.errorOnProtocolClose = errors.New("protocol close failed")
			case "tag":
				conn.tagError = errors.New("tag failed")
			}
			p := &lifecycleProgress{}
			result, err := executeDirectConnectionCycle(context.Background(), func() (driver.Connector, error) { return lifecycleSessionConnector{conn: conn}, nil }, initializeLifecycleSession("cnx-unit"), p.record)
			if (kind == "success") != (err == nil) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			counts := p.snapshot()
			if counts.SuccessfulConnections != 1 || counts.ConnectionCloses != 1 || c.closed.Load() != 1 {
				t.Fatalf("connection cleanup missing: %+v", counts)
			}
			if kind == "success" && (!conn.tagged || counts.TCPSVerifications != 1 || counts.RowCloses != 1 || c.rowsClosed.Load() != 2) {
				t.Fatalf("session/row cleanup missing: %+v", counts)
			}
			if kind != "success" && counts.CompletedCycles != 0 {
				t.Fatalf("failed initialization passed: %+v", counts)
			}
			wantTCPS, wantTags, wantQueries, wantFailures, wantRows := uint64(0), uint64(0), uint64(0), uint64(1), int64(1)
			if kind == "success" || kind == "tag" {
				wantTCPS = 1
			}
			if kind == "success" {
				wantTags, wantQueries, wantFailures, wantRows = 1, 1, 0, 2
			} else if kind == "protocol query" {
				wantRows = 0
			}
			if counts.TCPSVerifications != wantTCPS || counts.SessionIdentifications != wantTags || counts.QueryCompletions != wantQueries || counts.FailedCycles != wantFailures || c.queries.Load() != int64(wantQueries) || c.rowsClosed.Load() != wantRows {
				t.Fatalf("incorrect initialization boundary counts: %+v queries=%d row_close_calls=%d", counts, c.queries.Load(), c.rowsClosed.Load())
			}
		})
	}
	t.Run("primary and cleanup errors preserved", func(t *testing.T) {
		primary, cleanup := errors.New("initialization failed"), errors.New("close failed")
		c := &retentionFakeConnector{closeError: cleanup}
		_, err := executeDirectConnectionCycle(context.Background(), func() (driver.Connector, error) { return c, nil }, func(context.Context, driver.Conn, lifecycleEvent) error { return primary }, nil)
		if !errors.Is(err, primary) || !errors.Is(err, cleanup) || c.closed.Load() != 1 {
			t.Fatalf("got %v", err)
		}
	})
}

type lifecycleFailWriter struct{}

func (lifecycleFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// Inject finalization errors without changing the shared retention file helpers.
type lifecycleFinalizationFile struct {
	bytes.Buffer
	writeErr, syncErr, closeErr error
	calls                       []string
}

func (f *lifecycleFinalizationFile) Write(p []byte) (int, error) {
	f.calls = append(f.calls, "write")
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.Buffer.Write(p)
}
func (f *lifecycleFinalizationFile) Sync() error {
	f.calls = append(f.calls, "sync")
	return f.syncErr
}
func (f *lifecycleFinalizationFile) Close() error {
	f.calls = append(f.calls, "close")
	return f.closeErr
}

func TestConnectionLifecycleMeasurementsUnit(t *testing.T) {
	t.Run("lifecycle-local absolute paths", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		relative, err := filepath.Rel(cwd, dir)
		if err != nil {
			t.Fatal(err)
		}
		config := lifecycleUnitConfig()
		config.Workers, config.CyclesPerWorker = 1, 1
		config.Timeout, config.OutputDirectory = "15s", relative
		profiles := false
		config.DiagnosticProfiles = &profiles
		resolved, err := resolveLifecycleConfig(config)
		if err != nil || resolved.OutputDirectory != dir || config.OutputDirectory != relative {
			t.Fatalf("path resolution changed source or wrong destination: resolved=%+v source=%+v err=%v", resolved, config, err)
		}
		result, err := executeConnectionLifecycleStress(context.Background(), config, "cnx-relative", lifecycleStressOptions{
			Factory: func() (driver.Connector, error) { return &retentionFakeConnector{}, nil }, Initialize: lifecycleUnitInitializer,
		})
		wantArtifact := filepath.Join(resolved.OutputDirectory, "cnx-relative", "samples.jsonl")
		if err != nil || result.Artifact != wantArtifact || !filepath.IsAbs(result.Artifact) {
			t.Fatalf("announced/resolved/created paths differ: %+v %v", result, err)
		}
		f, err := os.Open(result.Artifact)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var metadata struct {
			Profile oracleTest.ConnectionLifecycleConfig `json:"profile"`
		}
		if err := json.NewDecoder(f).Decode(&metadata); err != nil || metadata.Profile.OutputDirectory != resolved.OutputDirectory {
			t.Fatalf("metadata path disagrees: %+v %v", metadata, err)
		}
	})
	for _, kind := range []string{"success", "write", "sync", "close", "combined", "workload"} {
		t.Run("finalization "+kind, func(t *testing.T) {
			writeErr, syncErr, closeErr, workloadErr := errors.New("write failed"), errors.New("sync failed"), errors.New("close failed"), errors.New("workload failed")
			f := &lifecycleFinalizationFile{}
			var prior error
			result := lifecycleStressResult{WorkloadStatus: "complete"}
			switch kind {
			case "write":
				f.writeErr = writeErr
			case "sync":
				f.syncErr = syncErr
			case "close":
				f.closeErr = closeErr
			case "combined":
				f.writeErr, f.syncErr, f.closeErr = writeErr, syncErr, closeErr
			case "workload":
				prior, result.WorkloadStatus = workloadErr, "failed"
			}
			r := &lifecycleRecorder{writer: f, runID: "cnx-finalize"}
			err := finalizeLifecycleArtifact(r, f, &result, nil, prior)
			for _, cause := range []error{f.writeErr, f.syncErr, f.closeErr, prior} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("cause lost: %v in %v", cause, err)
				}
			}
			wantStatus := "incomplete"
			if kind == "success" {
				wantStatus = "finalized"
			}
			if result.ArtifactStatus != wantStatus || (kind == "success") != (err == nil) || strings.Join(f.calls, ",") != "write,sync,close" {
				t.Fatalf("finalization result=%+v calls=%v err=%v", result, f.calls, err)
			}
			if f.writeErr == nil {
				var record struct {
					WorkloadStatus string `json:"workload_status"`
					ArtifactStatus string `json:"artifact_status"`
				}
				if e := json.Unmarshal(f.Bytes(), &record); e != nil || record.ArtifactStatus != "pending_finalization" || record.WorkloadStatus != result.WorkloadStatus {
					t.Fatalf("stream claimed finalization before Sync/Close: record=%+v err=%v", record, e)
				}
			}
		})
	}
	t.Run("enabled profiles and failure artifacts", func(t *testing.T) {
		config := lifecycleUnitConfig()
		config.Workers = 1
		config.CyclesPerWorker = 1
		config.Timeout = "15s"
		config.OutputDirectory = t.TempDir()
		factory := func() (driver.Connector, error) { return &retentionFakeConnector{}, nil }
		result, err := executeConnectionLifecycleStress(context.Background(), config, "cnx-profiles", lifecycleStressOptions{Factory: factory, Initialize: lifecycleUnitInitializer})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"baseline-heap.pprof", "final-heap.pprof", "settled-heap.pprof"} {
			data, err := os.ReadFile(filepath.Join(filepath.Dir(result.Artifact), name))
			if err != nil || len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
				t.Fatalf("invalid profile %s: %v", name, err)
			}
		}
		cause := errors.New("warmup initialization failed")
		result, err = executeConnectionLifecycleStress(context.Background(), config, "cnx-failure", lifecycleStressOptions{Factory: factory, Initialize: func(context.Context, driver.Conn, lifecycleEvent) error { return cause }})
		if !errors.Is(err, cause) || result.Warmup.Counts.SuccessfulConnections != result.Warmup.Counts.ConnectionCloses || result.Workload.Counts.CyclesStarted != 0 {
			t.Fatalf("warmup failure cleanup/result=%+v error=%v", result, err)
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(result.Artifact), "failure-goroutines.txt"))
		if err != nil || !bytes.Contains(data, []byte("goroutine")) {
			t.Fatalf("failure dump missing: %v", err)
		}
		data, err = os.ReadFile(result.Artifact)
		if err != nil || !bytes.Contains(data, []byte(`"workload_status":"not_started"`)) || result.ArtifactStatus != "incomplete" {
			t.Fatalf("failure result missing: %v", err)
		}
	})
	t.Run("streamed natural samples and sampler failure", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		r := &lifecycleRecorder{writer: lifecycleFailWriter{}, runID: "cnx-test", started: time.Now()}
		stop := startLifecycleSampler(ctx, time.Millisecond, r, &lifecycleProgress{}, cancel)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("write failure did not stop sampler")
		}
		if !errors.Is(stop(), io.ErrClosedPipe) || !errors.Is(stop(), io.ErrClosedPipe) {
			t.Fatal("sampler stop must preserve error and be idempotent")
		}
		var out bytes.Buffer
		r = &lifecycleRecorder{writer: &out, runID: "cnx-test", started: time.Now()}
		if _, err := r.sample("natural", &lifecycleProgress{}); err != nil {
			t.Fatal(err)
		}
		var sample lifecycleSample
		if err := json.Unmarshal(out.Bytes(), &sample); err != nil || sample.Phase != "natural" || sample.RunID != "cnx-test" || sample.PID != os.Getpid() {
			t.Fatalf("bad sample: %+v %v", sample, err)
		}
		if strings.Contains(out.String(), "pool") {
			t.Fatal("direct samples contain pool statistics")
		}
	})
	t.Run("warmup separated and settled endpoints", func(t *testing.T) {
		config := lifecycleUnitConfig()
		config.Workers = 2
		config.CyclesPerWorker = 3
		config.Timeout = "15s"
		config.SampleInterval = "1ms"
		config.OutputDirectory = t.TempDir()
		profiles := false
		config.DiagnosticProfiles = &profiles
		factory := func() (driver.Connector, error) {
			return &retentionFakeConnector{connect: func(ctx context.Context, _ int64) error {
				select {
				case <-time.After(3 * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}, nil
		}
		result, err := executeConnectionLifecycleStress(context.Background(), config, "cnx-artifact", lifecycleStressOptions{Factory: factory, Initialize: lifecycleUnitInitializer, ObserveFor: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if result.Warmup.Counts.CompletedCycles != 2 || result.Workload.Counts.CompletedCycles != 6 {
			t.Fatalf("warmup counted as measured work: %+v", result)
		}
		if result.WorkloadStatus != "complete" || result.ArtifactStatus != "finalized" {
			t.Fatalf("returned status before artifact finalization: %+v", result)
		}
		f, err := os.Open(result.Artifact)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		phases := map[string]bool{}
		decoder := json.NewDecoder(f)
		for {
			var event map[string]any
			err := decoder.Decode(&event)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			phase := event["phase"].(string)
			phases[phase] = true
			if phase == "result" && (event["workload_status"] != "complete" || event["artifact_status"] != "pending_finalization") {
				t.Fatalf("bad summary: %v", event)
			}
		}
		for _, phase := range []string{"configuration", "warmup", "baseline_natural", "baseline_post_gc", "natural", "final_natural", "final_post_gc", "settled_post_gc", "result"} {
			if !phases[phase] {
				t.Errorf("missing phase %s", phase)
			}
		}
		if _, err := os.Stat(filepath.Join(config.OutputDirectory, "cnx-artifact", "baseline-heap.pprof")); !os.IsNotExist(err) {
			t.Fatal("disabled profiles produced a heap file")
		}
		if _, err := executeConnectionLifecycleStress(context.Background(), config, "cnx-artifact", lifecycleStressOptions{Factory: factory}); err == nil {
			t.Fatal("run overwrote earlier evidence")
		}
	})
	t.Run("redact saved errors", func(t *testing.T) {
		config := &TestConfig{}
		config.Credentials.Password = "secret-test-password"
		config.Security.WalletLocation = "/secret/wallet"
		value := lifecycleSafeError(errors.New("secret-test-password /secret/wallet"), config)
		if strings.Contains(value, "secret-test-password") || strings.Contains(value, "/secret/wallet") {
			t.Fatal("secret in saved error")
		}
	})
}
