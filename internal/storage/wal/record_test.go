package wal

import (
	"errors"
	"testing"
	"time"
)

func TestRecordEncodeDecode(t *testing.T) {
	in := WALRecord{Operation: OperationPut, Timestamp: time.Unix(123, 456).UTC(), Key: []byte("key"), Value: []byte("value")}
	b, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Operation != in.Operation || !out.Timestamp.Equal(in.Timestamp) || string(out.Key) != "key" || string(out.Value) != "value" {
		t.Fatalf("decoded record = %#v", out)
	}
	b[0] ^= 1
	if _, err := Decode(b); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("error = %v", err)
	}
}

func TestDecodeChecksumAndEOF(t *testing.T) {
	b, _ := (WALRecord{Operation: OperationPut, Key: []byte("k")}).Encode()
	b[len(b)-1] ^= 1
	if _, err := Decode(b); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("error = %v", err)
	}
	if _, err := Decode(b[:10]); !errors.Is(err, ErrUnexpectedEOF) {
		t.Fatalf("error = %v", err)
	}
}
