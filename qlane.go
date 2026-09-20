package qlane

import (
	"bufio"
	"encoding/json"
	"os"
	"slices"
	"sort"
	"sync"
)

const defaultMaxAttempts = 3

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
	mu sync.Mutex
	// ready 始终按 ID（原始偏移）升序，新发布追加在尾，nack 按 ID 插回。
	ready       []Msg
	inflight    map[uint64]Msg
	keyInflight map[string]bool
	dead        []Msg
	nextID      uint64
	// MaxAttempts 达到后 nack 不再回主队列，转死信。
	MaxAttempts int
	path        string
	dlqPath     string
}

func New(path string) (*Queue, error) {
	q := &Queue{
		inflight:    map[uint64]Msg{},
		keyInflight: map[string]bool{},
		MaxAttempts: defaultMaxAttempts,
		path:        path,
		dlqPath:     path + ".dlq",
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
	if err := q.append(q.path, rec{Op: "pub", Msg: m}); err != nil {
		return err
	}
	q.ready = append(q.ready, m)
	return nil
}

func (q *Queue) Pull(now int64) (Msg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var blocked map[string]bool
	for i, m := range q.ready {
		if blocked[m.Key] {
			continue
		}
		if m.NotBefore > now || q.keyInflight[m.Key] {
			if blocked == nil {
				blocked = map[string]bool{}
			}
			blocked[m.Key] = true
			continue
		}
		if i == 0 {
			q.ready = q.ready[1:]
		} else {
			q.ready = append(q.ready[:i], q.ready[i+1:]...)
		}
		q.inflight[m.ID] = m
		q.keyInflight[m.Key] = true
		return m, true
	}
	return Msg{}, false
}

func (q *Queue) Ack(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, ok := q.inflight[id]
	if !ok {
		return nil
	}
	if err := q.append(q.path, rec{Op: "ack", Msg: m}); err != nil {
		return err
	}
	delete(q.inflight, id)
	delete(q.keyInflight, m.Key)
	return nil
}

func (q *Queue) Nack(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	m, ok := q.inflight[id]
	if !ok {
		return nil
	}
	m.Attempts++
	if m.Attempts >= q.MaxAttempts {
		// 先写死信再写主日志墓碑：中途崩溃宁可重复，不许丢。
		if err := q.append(q.dlqPath, rec{Op: "dead", Msg: m}); err != nil {
			return err
		}
		if err := q.append(q.path, rec{Op: "dead", Msg: m}); err != nil {
			return err
		}
		delete(q.inflight, id)
		delete(q.keyInflight, m.Key)
		q.dead = append(q.dead, m)
		return nil
	}
	if err := q.append(q.path, rec{Op: "nack", Msg: m}); err != nil {
		return err
	}
	delete(q.inflight, id)
	delete(q.keyInflight, m.Key)
	i := sort.Search(len(q.ready), func(i int) bool { return q.ready[i].ID > m.ID })
	q.ready = slices.Insert(q.ready, i, m)
	return nil
}

func (q *Queue) Dead() []Msg {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Msg, len(q.dead))
	copy(out, q.dead)
	return out
}

func (q *Queue) append(path string, row rec) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(row)
}

func (q *Queue) load() error {
	pos := map[uint64]int{}
	removed := map[uint64]bool{}
	err := q.scan(q.path, func(row rec) {
		switch row.Op {
		case "pub":
			pos[row.Msg.ID] = len(q.ready)
			q.ready = append(q.ready, row.Msg)
			if row.Msg.ID > q.nextID {
				q.nextID = row.Msg.ID
			}
		case "nack":
			if i, ok := pos[row.Msg.ID]; ok {
				q.ready[i].Attempts = row.Msg.Attempts
			}
		case "ack", "dead":
			removed[row.Msg.ID] = true
		}
	})
	if err != nil {
		return err
	}
	if len(removed) > 0 {
		kept := q.ready[:0]
		for _, m := range q.ready {
			if !removed[m.ID] {
				kept = append(kept, m)
			}
		}
		q.ready = kept
	}
	return q.scan(q.dlqPath, func(row rec) {
		if row.Op == "dead" {
			q.dead = append(q.dead, row.Msg)
		}
	})
}

func (q *Queue) scan(path string, fn func(rec)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var row rec
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			continue
		}
		fn(row)
	}
	return sc.Err()
}
