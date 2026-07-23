package service

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/document"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

func TestCreateIndexesDocument(t *testing.T) {
	svc := newTestService(t)

	id, err := svc.Create(context.Background(), document.Document{
		"name": "Alice",
		"age":  22,
		"meta": map[string]any{"team": "platform"},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	assertLookup(t, svc, "name", "Alice", []string{id})
	assertLookup(t, svc, "age", 22, []string{id})
	assertLookup(t, svc, "meta", "map[team:platform]", nil)
	assertLookup(t, svc, "_id", id, nil)
}

func TestDeleteRemovesIndexes(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := svc.Delete(context.Background(), id); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	assertLookup(t, svc, "city", "Chennai", nil)
}

func TestUpdateReplacesIndexesAndPreservesID(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{
		"city": "Chennai",
		"age":  22,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := svc.Update(context.Background(), id, document.Document{
		"_id":  "different-id",
		"city": "Bengaluru",
		"age":  23,
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	assertLookup(t, svc, "city", "Chennai", nil)
	assertLookup(t, svc, "city", "Bengaluru", []string{id})
	assertLookup(t, svc, "age", 22, nil)
	assertLookup(t, svc, "age", 23, []string{id})

	got, err := svc.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got["_id"] != id {
		t.Fatalf("stored _id = %v, want %q", got["_id"], id)
	}
}

func TestDuplicateIndexedValuesRemainAfterDelete(t *testing.T) {
	svc := newTestService(t)
	first, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	second, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	if err := svc.Delete(context.Background(), first); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	assertLookup(t, svc, "city", "Chennai", []string{second})
}

func TestCreateFailureDoesNotIndex(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.Create(context.Background(), document.Document{
		"broken": func() {},
	})
	if err == nil {
		t.Fatal("Create() error = nil, want marshal error")
	}
	if fields := svc.IndexManager().Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v after failed Create(), want empty", fields)
	}
}

func TestUpdateFailureLeavesIndexesConsistent(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = svc.Update(context.Background(), id, document.Document{"city": func() {}})
	if err == nil {
		t.Fatal("Update() error = nil, want marshal error")
	}
	assertLookup(t, svc, "city", "Chennai", []string{id})
	assertLookup(t, svc, "city", "Bengaluru", nil)
}

func TestDeleteFailureLeavesIndexesConsistent(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := svc.manager.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	err = svc.Delete(context.Background(), id)
	if err == nil {
		t.Fatal("Delete() error = nil, want storage error")
	}
	assertLookup(t, svc, "city", "Chennai", []string{id})
}

func TestFindByFieldSingleMatch(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	docs, err := svc.FindByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("FindByField() error = %v", err)
	}
	if len(docs) != 1 || docs[0]["_id"] != id {
		t.Fatalf("FindByField() = %v, want document %q", docs, id)
	}
}

func TestFindByFieldMultipleMatchesPreservesIndexOrder(t *testing.T) {
	svc := newTestService(t)
	first, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "first"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	second, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "second"})
	if err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	docs, err := svc.FindByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("FindByField() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("FindByField() returned %d documents, want 2", len(docs))
	}
	if docs[0]["_id"] != first || docs[1]["_id"] != second {
		t.Fatalf("FindByField() IDs = [%v %v], want [%s %s]", docs[0]["_id"], docs[1]["_id"], first, second)
	}
}

func TestFindByFieldNoMatchAndUnknownField(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Create(context.Background(), document.Document{"city": "Chennai"}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	for _, test := range []struct {
		name  string
		field string
		value any
	}{
		{name: "different value", field: "city", value: "Bengaluru"},
		{name: "unknown field", field: "country", value: "India"},
	} {
		t.Run(test.name, func(t *testing.T) {
			docs, err := svc.FindByField(context.Background(), test.field, test.value)
			if err != nil {
				t.Fatalf("FindByField() error = %v", err)
			}
			if docs != nil && len(docs) != 0 {
				t.Fatalf("FindByField() = %v, want no matches", docs)
			}
		})
	}

	doc, err := svc.FindOneByField(context.Background(), "country", "India")
	if err != nil {
		t.Fatalf("FindOneByField() error = %v", err)
	}
	if doc != nil {
		t.Fatalf("FindOneByField() = %v, want nil", doc)
	}
}

func TestFindByFieldSkipsMissingDocument(t *testing.T) {
	svc := newTestService(t)
	missingID, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "missing"})
	if err != nil {
		t.Fatalf("missing Create() error = %v", err)
	}
	remainingID, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "remaining"})
	if err != nil {
		t.Fatalf("remaining Create() error = %v", err)
	}

	if err := svc.manager.Delete(context.Background(), storage.Key(missingID)); err != nil {
		t.Fatalf("manager.Delete() error = %v", err)
	}

	docs, err := svc.FindByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("FindByField() error = %v", err)
	}
	if len(docs) != 1 || docs[0]["_id"] != remainingID {
		t.Fatalf("FindByField() = %v, want remaining document %q", docs, remainingID)
	}
}

func TestFindOneByFieldReturnsFirstMatch(t *testing.T) {
	svc := newTestService(t)
	first, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "first"})
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if _, err := svc.Create(context.Background(), document.Document{"city": "Chennai", "name": "second"}); err != nil {
		t.Fatalf("second Create() error = %v", err)
	}

	doc, err := svc.FindOneByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("FindOneByField() error = %v", err)
	}
	if doc == nil || (*doc)["_id"] != first {
		t.Fatalf("FindOneByField() = %v, want first document %q", doc, first)
	}
}

func TestFindByFieldAfterUpdateAndDelete(t *testing.T) {
	svc := newTestService(t)
	id, err := svc.Create(context.Background(), document.Document{"city": "Chennai"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := svc.Update(context.Background(), id, document.Document{"city": "Bengaluru"}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	assertFindCount(t, svc, "city", "Chennai", 0)
	assertFindCount(t, svc, "city", "Bengaluru", 1)

	if err := svc.Delete(context.Background(), id); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	assertFindCount(t, svc, "city", "Bengaluru", 0)
}

func TestFindByFieldPagination(t *testing.T) {
	svc := newTestService(t)
	for i := 0; i < 5; i++ {
		if _, err := svc.Create(context.Background(), document.Document{
			"city": "Chennai",
			"rank": i,
		}); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}

	all, err := svc.FindByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("FindByField() error = %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("FindByField() returned %d documents, want 5", len(all))
	}

	limited, err := svc.FindByField(context.Background(), "city", "Chennai", 2, 0)
	if err != nil {
		t.Fatalf("limited FindByField() error = %v", err)
	}
	assertDocumentIDs(t, limited, all[0]["_id"].(string), all[1]["_id"].(string))

	offset, err := svc.FindByField(context.Background(), "city", "Chennai", -1, 3)
	if err != nil {
		t.Fatalf("offset FindByField() error = %v", err)
	}
	assertDocumentIDs(t, offset, all[3]["_id"].(string), all[4]["_id"].(string))

	window, err := svc.FindByField(context.Background(), "city", "Chennai", 2, 1)
	if err != nil {
		t.Fatalf("window FindByField() error = %v", err)
	}
	assertDocumentIDs(t, window, all[1]["_id"].(string), all[2]["_id"].(string))

	larger, err := svc.FindByField(context.Background(), "city", "Chennai", 10, 0)
	if err != nil {
		t.Fatalf("larger-limit FindByField() error = %v", err)
	}
	if len(larger) != 5 {
		t.Fatalf("larger-limit FindByField() returned %d documents, want 5", len(larger))
	}

	beyond, err := svc.FindByField(context.Background(), "city", "Chennai", 2, 10)
	if err != nil {
		t.Fatalf("beyond-offset FindByField() error = %v", err)
	}
	if len(beyond) != 0 {
		t.Fatalf("beyond-offset FindByField() returned %d documents, want 0", len(beyond))
	}
}

func TestFindByFieldSorting(t *testing.T) {
	svc := newTestService(t)
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "name": "bravo", "age": 30, "active": true})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "name": "alpha", "age": 10, "active": false})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "name": "charlie", "age": 20, "active": true})
	createServiceDocument(t, svc, document.Document{"city": "Chennai"})

	docs, err := svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "name")
	if err != nil {
		t.Fatalf("ascending string FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "name", "alpha", "bravo", "charlie", nil)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "-name")
	if err != nil {
		t.Fatalf("descending string FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "name", "charlie", "bravo", "alpha", nil)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "age")
	if err != nil {
		t.Fatalf("ascending numeric FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "age", float64(10), float64(20), float64(30), nil)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "-age")
	if err != nil {
		t.Fatalf("descending numeric FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "age", float64(30), float64(20), float64(10), nil)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "active")
	if err != nil {
		t.Fatalf("ascending bool FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "active", false, true, true, nil)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "-active")
	if err != nil {
		t.Fatalf("descending bool FindByField() error = %v", err)
	}
	assertDocumentFieldValues(t, docs, "active", true, true, false, nil)
}

func TestFindByFieldSortingIsStableAndPrecedesPagination(t *testing.T) {
	svc := newTestService(t)
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "score": 10})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "score": 10})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "score": 5})
	original, err := svc.FindByField(context.Background(), "city", "Chennai")
	if err != nil {
		t.Fatalf("original FindByField() error = %v", err)
	}
	equalFirstID := original[0]["_id"].(string)
	if original[0]["score"] != float64(10) {
		equalFirstID = original[1]["_id"].(string)
	}

	docs, err := svc.FindByField(context.Background(), "city", "Chennai", 1, 1, "score")
	if err != nil {
		t.Fatalf("sorted paginated FindByField() error = %v", err)
	}
	assertDocumentIDs(t, docs, equalFirstID)

	docs, err = svc.FindByField(context.Background(), "city", "Chennai", -1, 0, "unknown")
	if err != nil {
		t.Fatalf("unknown-field FindByField() error = %v", err)
	}
	assertDocumentIDs(t, docs, original[0]["_id"].(string), original[1]["_id"].(string), original[2]["_id"].(string))
}

func TestAggregateByField(t *testing.T) {
	svc := newTestService(t)
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "age": 10})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "age": 20})
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "age": "not numeric"})
	createServiceDocument(t, svc, document.Document{"city": "Chennai"})
	createServiceDocument(t, svc, document.Document{"city": "Bengaluru", "age": 100})

	tests := []struct {
		aggregate string
		field     string
		want      any
	}{
		{aggregate: "count", want: 4},
		{aggregate: "sum:age", field: "sum", want: float64(30)},
		{aggregate: "avg:age", field: "average", want: float64(15)},
		{aggregate: "min:age", field: "minimum", want: float64(10)},
		{aggregate: "max:age", field: "maximum", want: float64(20)},
	}

	for _, tc := range tests {
		t.Run(tc.aggregate, func(t *testing.T) {
			result, err := svc.AggregateByField(context.Background(), "city", "Chennai", tc.aggregate, "")
			if err != nil {
				t.Fatalf("AggregateByField() error = %v", err)
			}
			if result[tc.field] != tc.want && tc.aggregate != "count" {
				t.Fatalf("AggregateByField() = %v, want %v", result, tc.want)
			}
			if tc.aggregate == "count" && result["count"] != tc.want {
				t.Fatalf("AggregateByField() = %v, want count %v", result, tc.want)
			}
		})
	}
}

func TestAggregateByFieldEmptyNumericResult(t *testing.T) {
	svc := newTestService(t)
	createServiceDocument(t, svc, document.Document{"city": "Chennai", "age": "not numeric"})

	result, err := svc.AggregateByField(context.Background(), "city", "Chennai", "sum:age", "")
	if err != nil {
		t.Fatalf("AggregateByField() error = %v", err)
	}
	if result["field"] != "age" || result["value"] != nil {
		t.Fatalf("AggregateByField() = %v, want field age and null value", result)
	}

	result, err = svc.AggregateByField(context.Background(), "city", "Unknown", "count", "")
	if err != nil {
		t.Fatalf("empty count AggregateByField() error = %v", err)
	}
	if result["count"] != 0 {
		t.Fatalf("empty count = %v, want 0", result)
	}
}

func createServiceDocument(t *testing.T, svc *Service, doc document.Document) string {
	t.Helper()

	id, err := svc.Create(context.Background(), doc)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return id
}

func newTestService(t *testing.T) *Service {
	t.Helper()

	mgr, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("manager.New() error = %v", err)
	}
	if err := mgr.Open(context.Background()); err != nil {
		t.Fatalf("manager.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("manager.Close() error = %v", err)
		}
	})
	return New(mgr)
}

func assertLookup(t *testing.T, svc *Service, field string, value any, want []string) {
	t.Helper()

	got := svc.IndexManager().Lookup(field, value)
	if len(got) != len(want) {
		t.Fatalf("Lookup(%q, %v) = %v, want %v", field, value, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Lookup(%q, %v) = %v, want %v", field, value, got, want)
		}
	}
}

func assertFindCount(t *testing.T, svc *Service, field string, value any, want int) {
	t.Helper()

	docs, err := svc.FindByField(context.Background(), field, value)
	if err != nil {
		t.Fatalf("FindByField() error = %v", err)
	}
	if len(docs) != want {
		t.Fatalf("FindByField(%q, %v) returned %d documents, want %d", field, value, len(docs), want)
	}
}

func assertDocumentIDs(t *testing.T, docs []document.Document, want ...string) {
	t.Helper()

	if len(docs) != len(want) {
		t.Fatalf("got %d documents, want %d", len(docs), len(want))
	}
	for i, doc := range docs {
		if doc["_id"] != want[i] {
			t.Fatalf("document %d ID = %v, want %q", i, doc["_id"], want[i])
		}
	}
}

func assertDocumentFieldValues(t *testing.T, docs []document.Document, field string, want ...any) {
	t.Helper()

	if len(docs) != len(want) {
		t.Fatalf("got %d documents, want %d", len(docs), len(want))
	}
	for i, doc := range docs {
		if doc[field] != want[i] {
			t.Fatalf("document %d %q = %v, want %v", i, field, doc[field], want[i])
		}
	}
}
