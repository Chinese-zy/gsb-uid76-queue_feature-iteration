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
