package qlane

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"os"
	"sync"
)

// MaxAttempts is how many times a message may be nacked before it
// leaves the main queue for the dead-letter path.
const MaxAttempts = 3

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

// msgHeap orders delayed messages by NotBefore, breaking ties by ID so
// messages due at the same time keep their publish (offset) order.
type msgHeap []Msg

func (h msgHeap) Len() int { return len(h) }
func (h msgHeap) Less(i, j int) bool {
	if h[i].NotBefore != h[j].NotBefore {
		return h[i].NotBefore < h[j].NotBefore
	}
	return h[i].ID < h[j].ID
}
func (h msgHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *msgHeap) Push(x any)   { *h = append(*h, x.(Msg)) }
func (h *msgHeap) Pop() any {
	old := *h
	n := len(old)
	m := old[n-1]
	*h = old[:n-1]
	return m
}

type Queue struct {
	mu       sync.Mutex
	ready    []Msg            // due and deliverable, in the order they became ready
	delayed  msgHeap          // active but not yet due
	waiting  map[string][]Msg // per-key backlog behind the key's active message
	active   map[string]bool  // keys with a message in ready, delayed or inflight
	inflight map[uint64]Msg
	dead     []Msg
	nextID   uint64
	lastNow  int64
	path     string
}

func New(path string) (*Queue, error) {
	q := &Queue{
		waiting:  map[string][]Msg{},
		active:   map[string]bool{},
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
	if err := q.append(rec{Op: "pub", Msg: m}); err != nil {
		return err
	}
	q.enqueue(m)
	return nil
}

func (q *Queue) Pull(now int64) (Msg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if now > q.lastNow {
		q.lastNow = now
	}
	for len(q.delayed) > 0 && q.delayed[0].NotBefore <= q.lastNow {
		q.ready = append(q.ready, heap.Pop(&q.delayed).(Msg))
	}
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
	if err := q.append(rec{Op: "ack", Msg: m}); err != nil {
		return err
	}
	delete(q.inflight, id)
	q.advance(m.Key)
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
	if m.Attempts >= MaxAttempts {
		if err := q.append(rec{Op: "dead", Msg: m}); err != nil {
			return err
		}
		delete(q.inflight, id)
		q.dead = append(q.dead, m)
		q.advance(m.Key)
		return nil
	}
	if err := q.append(rec{Op: "nack", Msg: m}); err != nil {
		return err
	}
	delete(q.inflight, id)
	q.activate(m)
	return nil
}

// PullDead takes the next message off the dead-letter path. Dead letters
// never rejoin the main queue.
func (q *Queue) PullDead() (Msg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.dead) == 0 {
		return Msg{}, false
	}
	m := q.dead[0]
	q.dead = q.dead[1:]
	return m, true
}

// enqueue routes a freshly published or recovered message: behind the
// key's active message if the key is occupied, otherwise active.
func (q *Queue) enqueue(m Msg) {
	if q.active[m.Key] {
		q.waiting[m.Key] = append(q.waiting[m.Key], m)
		return
	}
	q.activate(m)
}

// activate makes m its key's head message: straight to the ready tail
// when already due, otherwise onto the delayed heap. It never cuts in
// front of messages that became ready earlier.
func (q *Queue) activate(m Msg) {
	q.active[m.Key] = true
	if m.NotBefore <= q.lastNow {
		q.ready = append(q.ready, m)
		return
	}
	heap.Push(&q.delayed, m)
}

// advance unblocks a key after its head message was acked or dead-lettered.
func (q *Queue) advance(key string) {
	if w := q.waiting[key]; len(w) > 0 {
		m := w[0]
		w = w[1:]
		if len(w) == 0 {
			delete(q.waiting, key)
		} else {
			q.waiting[key] = w
		}
		q.activate(m)
		return
	}
	delete(q.active, key)
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
	pending := map[uint64]Msg{}
	var order []uint64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var row rec
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			continue
		}
		switch row.Op {
		case "pub":
			if _, ok := pending[row.Msg.ID]; !ok {
				order = append(order, row.Msg.ID)
			}
			pending[row.Msg.ID] = row.Msg
		case "nack":
			if _, ok := pending[row.Msg.ID]; ok {
				pending[row.Msg.ID] = row.Msg
			}
		case "ack":
			delete(pending, row.Msg.ID)
		case "dead":
			if _, ok := pending[row.Msg.ID]; ok {
				delete(pending, row.Msg.ID)
				q.dead = append(q.dead, row.Msg)
			}
		}
		if row.Msg.ID > q.nextID {
			q.nextID = row.Msg.ID
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	// Redeliver everything that was never acked, in original offset
	// order. At-least-once: inflight-at-crash messages reappear here.
	for _, id := range order {
		if m, ok := pending[id]; ok {
			q.enqueue(m)
		}
	}
	return nil
}
