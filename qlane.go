package qlane

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type Msg struct {
	ID        uint64 `json:"id"`
	Key       string `json:"key"`
	Body      []byte `json:"body"`
	NotBefore int64  `json:"not_before"`
	Attempts  int    `json:"attempts"`
}

type rec struct {
	Op  string `json:"op"`
	Msg Msg    `json:"msg"`
}

type Queue struct {
	mu       sync.Mutex
	ready    []Msg
	inflight map[uint64]Msg
	nextID   uint64
	path     string
}

func New(path string) (*Queue, error) {
	q := &Queue{
		inflight: map[uint64]Msg{},
		path:     path,
	}
	if err := q.load(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) Publish(m Msg) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	m.ID = q.nextID
	q.ready = append(q.ready, m)
	return q.append(rec{Op: "pub", Msg: m})
}

func (q *Queue) Pull(now int64) (Msg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_ = now
	if len(q.ready) == 0 {
		return Msg{}, false
	}
	m := q.ready[0]
	q.ready = q.ready[1:]
	q.inflight[m.ID] = m
	return m, true
}

func (q *Queue) Ack(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, ok := q.inflight[id]
	if !ok {
		return nil
	}
	delete(q.inflight, id)
	return q.append(rec{Op: "ack", Msg: m})
}

func (q *Queue) Nack(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, ok := q.inflight[id]
	if !ok {
		return nil
	}
	delete(q.inflight, id)
	m.Attempts++
	q.ready = append([]Msg{m}, q.ready...)
	return q.append(rec{Op: "nack", Msg: m})
}

func (q *Queue) append(row rec) error {
	f, err := os.OpenFile(q.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(row)
}

func (q *Queue) load() error {
	f, err := os.Open(q.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	var pubs []Msg
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var row rec
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			continue
		}
		if row.Op != "pub" {
			continue
		}
		pubs = append(pubs, row.Msg)
		if row.Msg.ID > q.nextID {
			q.nextID = row.Msg.ID
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	for i, j := 0, len(pubs)-1; i < j; i, j = i+1, j-1 {
		pubs[i], pubs[j] = pubs[j], pubs[i]
	}
	q.ready = pubs
	return nil
}
