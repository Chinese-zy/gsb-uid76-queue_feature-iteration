package qlane

import (
	"path/filepath"
	"testing"
)

func open(t *testing.T) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.log")
	q, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	return q, path
}

func reopen(t *testing.T, path string) *Queue {
	t.Helper()
	q, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func mustPublish(t *testing.T, q *Queue, m Msg) {
	t.Helper()
	if err := q.Publish(m); err != nil {
		t.Fatal(err)
	}
}

func mustPull(t *testing.T, q *Queue, now int64) Msg {
	t.Helper()
	m, ok := q.Pull(now)
	if !ok {
		t.Fatal("expected a message")
	}
	return m
}

func mustAck(t *testing.T, q *Queue, id uint64) {
	t.Helper()
	if err := q.Ack(id); err != nil {
		t.Fatal(err)
	}
}

func mustNack(t *testing.T, q *Queue, id uint64) {
	t.Helper()
	if err := q.Nack(id); err != nil {
		t.Fatal(err)
	}
}

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

func TestDelayedWaitsForItsTime(t *testing.T) {
	q, _ := open(t)
	mustPublish(t, q, Msg{Key: "慢", Body: []byte("慢"), NotBefore: 100})
	mustPublish(t, q, Msg{Key: "快", Body: []byte("快")})
	if m := mustPull(t, q, 0); string(m.Body) != "快" {
		t.Fatalf("延迟消息不该挡住其他键, got %s", m.Body)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("没到点不该投递")
	}
	if _, ok := q.Pull(99); ok {
		t.Fatal("没到点不该投递")
	}
	if m := mustPull(t, q, 100); string(m.Body) != "慢" {
		t.Fatalf("到点应投递, got %s", m.Body)
	}
}

func TestDelayedKeepsOffsetOrder(t *testing.T) {
	q, _ := open(t)
	mustPublish(t, q, Msg{Key: "甲", Body: []byte("甲")})
	mustPublish(t, q, Msg{Key: "乙", Body: []byte("乙"), NotBefore: 50})
	if m := mustPull(t, q, 60); string(m.Body) != "甲" {
		t.Fatalf("到点后按原始偏移排队, got %s", m.Body)
	}
	if m := mustPull(t, q, 60); string(m.Body) != "乙" {
		t.Fatalf("延迟消息不许插到先发布的普通消息前面, got %s", m.Body)
	}
}

func TestSameKeySerializedUntilAck(t *testing.T) {
	q, _ := open(t)
	mustPublish(t, q, Msg{Key: "k", Body: []byte("1")})
	mustPublish(t, q, Msg{Key: "k", Body: []byte("2")})
	m1 := mustPull(t, q, 0)
	if _, ok := q.Pull(0); ok {
		t.Fatal("前一条未确认, 后一条只能等")
	}
	mustAck(t, q, m1.ID)
	if m := mustPull(t, q, 0); string(m.Body) != "2" {
		t.Fatalf("确认后按序走下一条, got %s", m.Body)
	}
}

func TestSameKeyDelayedHeadBlocksLater(t *testing.T) {
	q, _ := open(t)
	mustPublish(t, q, Msg{Key: "k", Body: []byte("1"), NotBefore: 100})
	mustPublish(t, q, Msg{Key: "k", Body: []byte("2")})
	if _, ok := q.Pull(0); ok {
		t.Fatal("同键前一条没到点, 后一条就算到点也得等")
	}
	m1 := mustPull(t, q, 100)
	if string(m1.Body) != "1" {
		t.Fatalf("同键按序, got %s", m1.Body)
	}
	if _, ok := q.Pull(100); ok {
		t.Fatal("前一条未确认, 后一条只能等")
	}
	mustAck(t, q, m1.ID)
	if m := mustPull(t, q, 100); string(m.Body) != "2" {
		t.Fatalf("got %s", m.Body)
	}
}

func TestNackKeepsKeyOrder(t *testing.T) {
	q, _ := open(t)
	mustPublish(t, q, Msg{Key: "k", Body: []byte("1")})
	mustPublish(t, q, Msg{Key: "k", Body: []byte("2")})
	m1 := mustPull(t, q, 0)
	mustNack(t, q, m1.ID)
	m := mustPull(t, q, 0)
	if string(m.Body) != "1" || m.Attempts != 1 {
		t.Fatalf("nack 后应按原偏移重投, got %s attempts=%d", m.Body, m.Attempts)
	}
	mustAck(t, q, m.ID)
	if m := mustPull(t, q, 0); string(m.Body) != "2" {
		t.Fatalf("got %s", m.Body)
	}
}

func TestRestartRedeliversUnackedInOrder(t *testing.T) {
	q, path := open(t)
	mustPublish(t, q, Msg{Key: "x", Body: []byte("甲")})
	mustPublish(t, q, Msg{Key: "y", Body: []byte("乙")})
	mustPublish(t, q, Msg{Key: "x", Body: []byte("丙")})
	m1 := mustPull(t, q, 0)
	m2 := mustPull(t, q, 0)
	mustAck(t, q, m2.ID)
	if _, ok := q.Pull(0); ok {
		t.Fatal("丙与甲同键, 甲未确认前不能走")
	}

	q2 := reopen(t, path)
	m := mustPull(t, q2, 0)
	if m.ID != m1.ID || string(m.Body) != "甲" {
		t.Fatalf("未确认的按原偏移重投, got id=%d body=%s", m.ID, m.Body)
	}
	if _, ok := q2.Pull(0); ok {
		t.Fatal("重启后同键顺序不许打乱")
	}
	mustAck(t, q2, m.ID)
	if m := mustPull(t, q2, 0); string(m.Body) != "丙" {
		t.Fatalf("got %s", m.Body)
	}
	if _, ok := q2.Pull(0); ok {
		t.Fatal("已确认的不许重投")
	}
}

func TestRestartKeepsNotBefore(t *testing.T) {
	q, path := open(t)
	mustPublish(t, q, Msg{Key: "d", Body: []byte("慢"), NotBefore: 100})
	q2 := reopen(t, path)
	if _, ok := q2.Pull(0); ok {
		t.Fatal("重启后没到点不该投递")
	}
	if m := mustPull(t, q2, 100); string(m.Body) != "慢" {
		t.Fatalf("got %s", m.Body)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	q, path := open(t)
	mustPublish(t, q, Msg{Key: "k", Body: []byte("坏")})
	for i := 0; i < 3; i++ {
		m := mustPull(t, q, 0)
		mustNack(t, q, m.ID)
	}
	if _, ok := q.Pull(0); ok {
		t.Fatal("重试次数到了不许再混进主队列")
	}
	dead := q.Dead()
	if len(dead) != 1 || string(dead[0].Body) != "坏" || dead[0].Attempts != 3 {
		t.Fatalf("死信单独一条路, got %+v", dead)
	}
	mustPublish(t, q, Msg{Key: "k", Body: []byte("新")})
	m := mustPull(t, q, 0)
	if string(m.Body) != "新" {
		t.Fatalf("死信不该卡住同键新消息, got %s", m.Body)
	}
	mustAck(t, q, m.ID)

	q2 := reopen(t, path)
	if _, ok := q2.Pull(0); ok {
		t.Fatal("重启后死信不许复活进主队列")
	}
	if dead := q2.Dead(); len(dead) != 1 || string(dead[0].Body) != "坏" {
		t.Fatalf("死信应持久化, got %+v", dead)
	}
}
