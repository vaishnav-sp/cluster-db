package index

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vaishnav-sp/cluster-db/internal/document"
)

func TestIndexManagerIndexAndLookup(t *testing.T) {
	mgr := NewIndexManager()

	doc := document.Document{
		"_id":  "doc1",
		"name": "Alice",
		"age":  float64(22),
		"city": "Chennai",
	}
	mgr.IndexDocument("doc1", doc)

	assertLookup(t, mgr, "age", 22, []string{"doc1"})
	assertLookup(t, mgr, "city", "Chennai", []string{"doc1"})
	assertLookup(t, mgr, "name", "Alice", []string{"doc1"})
	assertLookup(t, mgr, "_id", "doc1", nil)
	assertLookup(t, mgr, "missing", "x", nil)
}

func TestIndexManagerSkipsNestedFieldsAndID(t *testing.T) {
	mgr := NewIndexManager()

	doc := document.Document{
		"_id":    "doc1",
		"meta":   map[string]any{"k": "v"},
		"tags":   []any{"a", "b"},
		"active": true,
	}
	mgr.IndexDocument("doc1", doc)

	if fields := mgr.Fields(); len(fields) != 1 || fields[0] != "active" {
		t.Fatalf("Fields() = %v, want [active]", fields)
	}
	assertLookup(t, mgr, "active", true, []string{"doc1"})
	assertLookup(t, mgr, "meta", map[string]any{"k": "v"}, nil)
}

func TestIndexManagerDuplicateValues(t *testing.T) {
	mgr := NewIndexManager()

	mgr.IndexDocument("doc1", document.Document{"city": "Chennai"})
	mgr.IndexDocument("doc2", document.Document{"city": "Chennai"})

	assertLookup(t, mgr, "city", "Chennai", []string{"doc1", "doc2"})
}

func TestIndexManagerRemoveDocument(t *testing.T) {
	mgr := NewIndexManager()

	doc1 := document.Document{"city": "Chennai", "age": float64(22)}
	doc2 := document.Document{"city": "Chennai", "age": float64(30)}

	mgr.IndexDocument("doc1", doc1)
	mgr.IndexDocument("doc2", doc2)

	mgr.RemoveDocument("doc1", doc1)

	assertLookup(t, mgr, "city", "Chennai", []string{"doc2"})
	assertLookup(t, mgr, "age", 22, nil)
	assertLookup(t, mgr, "age", 30, []string{"doc2"})
}

func TestIndexManagerUpdateDocument(t *testing.T) {
	mgr := NewIndexManager()

	oldDoc := document.Document{"city": "Chennai", "age": float64(22)}
	newDoc := document.Document{"city": "Bengaluru", "age": float64(23)}

	mgr.IndexDocument("doc1", oldDoc)
	mgr.UpdateDocument("doc1", oldDoc, newDoc)

	assertLookup(t, mgr, "city", "Chennai", nil)
	assertLookup(t, mgr, "city", "Bengaluru", []string{"doc1"})
	assertLookup(t, mgr, "age", 22, nil)
	assertLookup(t, mgr, "age", 23, []string{"doc1"})
}

func TestIndexManagerCleanupEmptyIndexes(t *testing.T) {
	mgr := NewIndexManager()

	doc := document.Document{"city": "Chennai"}
	mgr.IndexDocument("doc1", doc)
	if len(mgr.Fields()) != 1 {
		t.Fatalf("expected one indexed field before remove")
	}

	mgr.RemoveDocument("doc1", doc)

	if fields := mgr.Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v, want empty after remove", fields)
	}
	assertLookup(t, mgr, "city", "Chennai", nil)
}

func TestIndexManagerFieldsSorted(t *testing.T) {
	mgr := NewIndexManager()

	mgr.IndexDocument("doc1", document.Document{"zebra": 1, "alpha": 2, "middle": 3})

	want := []string{"alpha", "middle", "zebra"}
	got := mgr.Fields()
	if !sort.StringsAreSorted(got) {
		t.Fatalf("Fields() not sorted: %v", got)
	}
	if len(got) != len(want) {
		t.Fatalf("Fields() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Fields() = %v, want %v", got, want)
		}
	}
}

func TestIndexManagerConcurrentAccess(t *testing.T) {
	mgr := NewIndexManager()
	const workers = 32
	const docsPerWorker = 50

	var lookupFailures atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < docsPerWorker; i++ {
				id := fmt.Sprintf("w%d-s%d", w, i)
				doc := document.Document{
					"worker": float64(w),
					"seq":    float64(i),
					"city":   "Chennai",
				}
				mgr.IndexDocument(id, doc)

				if got := mgr.Lookup("city", "Chennai"); len(got) == 0 {
					lookupFailures.Add(1)
				}

				updated := document.Document{
					"worker": float64(w),
					"seq":    float64(i),
					"city":   "Madurai",
				}
				mgr.UpdateDocument(id, doc, updated)
				mgr.RemoveDocument(id, updated)
			}
		}()
	}

	wg.Wait()

	if lookupFailures.Load() > 0 {
		t.Fatalf("lookup returned empty %d times during concurrent indexing", lookupFailures.Load())
	}
	if fields := mgr.Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v, want empty after concurrent remove", fields)
	}
}

func assertLookup(t *testing.T, mgr *IndexManager, field string, value any, want []string) {
	t.Helper()

	got := mgr.Lookup(field, value)
	if len(got) != len(want) {
		t.Fatalf("Lookup(%q, %v) = %v, want %v", field, value, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Lookup(%q, %v) = %v, want %v", field, value, got, want)
		}
	}
}
