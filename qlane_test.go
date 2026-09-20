package qlane

import (
	"path/filepath"
	"testing"
)

func TestImmediateDifferentKeys(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"甲", "乙", "丙"} {
		if err := q.Publish(Msg{Key: body, Body: []byte(body), NotBefore: 0}); err != nil {
			t.Fatal(err)
		}
	}
	var got string
	for {
		m, ok := q.Pull(0)
		if !ok {
			break
		}
		got += string(m.Body)
		if err := q.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if got != "甲乙丙" {
		t.Fatalf("got %s", got)
	}
}

func TestAckRemoves(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("x"), NotBefore: 0}); err != nil {
		t.Fatal(err)
	}
	m, ok := q.Pull(0)
	if !ok {
		t.Fatal("missing")
	}
	if err := q.Ack(m.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("still queued")
	}
}

func TestDelayedWaitsForNotBefore(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("d"), NotBefore: 100}); err != nil {
		t.Fatal(err)
	}
	for _, now := range []int64{0, 50, 99} {
		if _, ok := q.Pull(now); ok {
			t.Fatalf("delivered at %d before not_before", now)
		}
	}
	m, ok := q.Pull(100)
	if !ok || string(m.Body) != "d" {
		t.Fatalf("not delivered at not_before: %v %v", m, ok)
	}
}

func TestDelayedDoesNotCutLine(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "a", Body: []byte("a"), NotBefore: 0}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "b", Body: []byte("b"), NotBefore: 0}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "c", Body: []byte("c"), NotBefore: 5}); err != nil {
		t.Fatal(err)
	}
	// The delayed message must not jump ahead of already-ready ones.
	for _, want := range []string{"a", "b"} {
		m, ok := q.Pull(10)
		if !ok || string(m.Body) != want {
			t.Fatalf("want %s, got %v", want, m)
		}
	}
	m, ok := q.Pull(10)
	if !ok || string(m.Body) != "c" {
		t.Fatalf("delayed message missing: %v", m)
	}
}

func TestSameKeyWaitsForAck(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []Msg{
		{Key: "k", Body: []byte("1")},
		{Key: "k", Body: []byte("2")},
		{Key: "j", Body: []byte("3")},
	} {
		if err := q.Publish(m); err != nil {
			t.Fatal(err)
		}
	}
	first, ok := q.Pull(0)
	if !ok || string(first.Body) != "1" {
		t.Fatalf("first pull: %v", first)
	}
	// Different key flows; same key is blocked behind the unacked one.
	other, ok := q.Pull(0)
	if !ok || string(other.Body) != "3" {
		t.Fatalf("other key blocked: %v", other)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("same key delivered before ack")
	}
	if err := q.Ack(first.ID); err != nil {
		t.Fatal(err)
	}
	m, ok := q.Pull(0)
	if !ok || string(m.Body) != "2" {
		t.Fatalf("after ack: %v", m)
	}
}

func TestSameKeyDelayedKeepsOrder(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("1"), NotBefore: 100}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("2"), NotBefore: 0}); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("second message overtook delayed first")
	}
	m, ok := q.Pull(100)
	if !ok || string(m.Body) != "1" {
		t.Fatalf("want 1, got %v", m)
	}
	if _, ok := q.Pull(100); ok {
		t.Fatal("second message delivered before ack")
	}
	if err := q.Ack(m.ID); err != nil {
		t.Fatal(err)
	}
	m, ok = q.Pull(100)
	if !ok || string(m.Body) != "2" {
		t.Fatalf("want 2, got %v", m)
	}
}

func TestNackRequeuesBehindOthers(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "a", Body: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "b", Body: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	ma, _ := q.Pull(0)
	mb, _ := q.Pull(0)
	if err := q.Nack(ma.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(mb.ID); err != nil {
		t.Fatal(err)
	}
	m, ok := q.Pull(0)
	if !ok || string(m.Body) != "a" || m.Attempts != 1 {
		t.Fatalf("nacked message: %v", m)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	q, err := New(filepath.Join(t.TempDir(), "q.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("p")}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("next")}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxAttempts; i++ {
		m, ok := q.Pull(0)
		if !ok || string(m.Body) != "p" {
			t.Fatalf("attempt %d: %v", i, m)
		}
		if err := q.Nack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	// Out of retries: off the main queue, onto the dead-letter path.
	d, ok := q.PullDead()
	if !ok || string(d.Body) != "p" || d.Attempts != MaxAttempts {
		t.Fatalf("dead letter: %v", d)
	}
	// The key is unblocked; the next message flows.
	m, ok := q.Pull(0)
	if !ok || string(m.Body) != "next" {
		t.Fatalf("after dead letter: %v", m)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("dead letter back in main queue")
	}
}

func TestRestartRedeliversUnackedInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")
	q, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []Msg{
		{Key: "k", Body: []byte("h1")},
		{Key: "k", Body: []byte("h2")},
		{Key: "j", Body: []byte("h3")},
		{Key: "j", Body: []byte("h4")},
	} {
		if err := q.Publish(m); err != nil {
			t.Fatal(err)
		}
	}
	m1, _ := q.Pull(0)
	if err := q.Ack(m1.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Pull(0); !ok { // h3 left inflight at "crash"
		t.Fatal("missing h3")
	}

	q2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	// Unacked messages come back in original offset order; the acked
	// one stays gone; per-key order is preserved.
	for _, want := range []string{"h2", "h3", "h4"} {
		m, ok := q2.Pull(0)
		if !ok || string(m.Body) != want {
			t.Fatalf("want %s, got %v", want, m)
		}
		if err := q2.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := q2.Pull(0); ok {
		t.Fatal("acked message redelivered")
	}
}

func TestRestartKeepsAttemptsAndDelay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")
	q, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("p")}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "d", Body: []byte("d"), NotBefore: 100}); err != nil {
		t.Fatal(err)
	}
	m, _ := q.Pull(0)
	if err := q.Nack(m.ID); err != nil { // attempts = 1
		t.Fatal(err)
	}

	q2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	m2, ok := q2.Pull(50)
	if !ok || string(m2.Body) != "p" || m2.Attempts != 1 {
		t.Fatalf("nacked message after restart: %v", m2)
	}
	if _, ok := q2.Pull(50); ok {
		t.Fatal("delayed message delivered early after restart")
	}
	m2, ok = q2.Pull(100)
	if !ok || string(m2.Body) != "d" {
		t.Fatalf("delayed message after restart: %v", m2)
	}
	if _, ok := q2.Pull(100); ok {
		t.Fatal("unexpected extra message")
	}
}

func TestDeadLetterSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.log")
	q, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(Msg{Key: "k", Body: []byte("p")}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxAttempts; i++ {
		m, ok := q.Pull(0)
		if !ok {
			t.Fatal("missing message")
		}
		if err := q.Nack(m.ID); err != nil {
			t.Fatal(err)
		}
	}

	q2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q2.Pull(0); ok {
		t.Fatal("dead letter rejoined main queue after restart")
	}
	d, ok := q2.PullDead()
	if !ok || string(d.Body) != "p" {
		t.Fatalf("dead letter lost across restart: %v", d)
	}
}
