package wal

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

type syncBuffer struct {
	bytes.Buffer
	synced bool
}

func (b *syncBuffer) Sync() error { b.synced = true; return nil }

func TestWriterAppendSyncAndClose(t *testing.T) {
	b := &syncBuffer{}
	w, err := NewWriter(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(WALRecord{Operation: OperationPut, Key: []byte("a"), Value: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil || !b.synced {
		t.Fatalf("sync = %v, synced=%v", err, b.synced)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(WALRecord{Operation: OperationPut}); !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v", err)
	}
}

func TestWriterConcurrentAppend(t *testing.T) {
	var b bytes.Buffer
	w, err := NewWriter(&b)
	if err != nil {
		t.Fatal(err)
	}
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Append(WALRecord{Operation: OperationPut, Key: []byte("k")}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	r, _ := NewReader(&b)
	count := 0
	for r.Next() {
		count++
	}
	if err := r.Error(); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("records = %d", count)
	}
}
