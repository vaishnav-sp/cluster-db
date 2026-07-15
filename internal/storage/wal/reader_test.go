package wal

import (
	"bytes"
	"errors"
	"testing"
)

func TestReaderIterationAndEmpty(t *testing.T) {
	var b bytes.Buffer
	w, _ := NewWriter(&b)
	_ = w.Append(WALRecord{Operation: OperationPut, Key: []byte("a"), Value: []byte("1")})
	_ = w.Append(WALRecord{Operation: OperationDelete, Key: []byte("a")})
	r, _ := NewReader(&b)
	if !r.Next() || string(r.Record().Value) != "1" {
		t.Fatal("first record missing")
	}
	if !r.Next() || r.Record().Operation != OperationDelete {
		t.Fatal("second record missing")
	}
	if r.Next() || r.Error() != nil {
		t.Fatalf("end error = %v", r.Error())
	}
	empty, _ := NewReader(bytes.NewReader(nil))
	if empty.Next() || empty.Error() != nil {
		t.Fatal("empty WAL should be clean EOF")
	}
}

func TestReaderUnexpectedEOFAndClose(t *testing.T) {
	r, _ := NewReader(bytes.NewReader([]byte{1, 2}))
	if r.Next() || !errors.Is(r.Error(), ErrUnexpectedEOF) {
		t.Fatalf("error = %v", r.Error())
	}
	r, _ = NewReader(bytes.NewReader(nil))
	_ = r.Close()
	if r.Next() || !errors.Is(r.Error(), ErrClosed) {
		t.Fatalf("error = %v", r.Error())
	}
}

func TestReaderInvalidMagic(t *testing.T) {
	b, _ := (WALRecord{Operation: OperationPut, Key: []byte("key")}).Encode()
	b[0] ^= 1
	r, _ := NewReader(bytes.NewReader(b))
	if r.Next() || !errors.Is(r.Error(), ErrInvalidMagic) {
		t.Fatalf("error = %v", r.Error())
	}
}
