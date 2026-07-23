// Package service bridges document operations to the KV storage manager.
// Each document is stored as a JSON blob keyed by its ULID identifier.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/vaishnav-sp/cluster-db/internal/document"
	"github.com/vaishnav-sp/cluster-db/internal/document/index"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// Service persists and retrieves documents through the storage manager.
type Service struct {
	manager *manager.Manager
	indexes *index.IndexManager
}

// New constructs a document Service backed by the given storage manager.
func New(mgr *manager.Manager) *Service {
	return &Service{
		manager: mgr,
		indexes: index.NewIndexManager(),
	}
}

// Create assigns a new ULID, embeds it as "_id", and stores the document as JSON.
func (s *Service) Create(ctx context.Context, doc document.Document) (string, error) {
	id := document.NewID()

	stored := make(document.Document, len(doc)+1)
	for k, v := range doc {
		stored[k] = v
	}
	stored["_id"] = id

	jsonBytes, err := json.Marshal(stored)
	if err != nil {
		return "", fmt.Errorf("document service: marshal document: %w", err)
	}

	rec := storage.Record{
		Key:   storage.Key(id),
		Value: storage.Value(jsonBytes),
	}
	if err := s.manager.Put(ctx, rec); err != nil {
		return "", fmt.Errorf("document service: create: %w", err)
	}
	s.indexes.IndexDocument(id, stored)

	return id, nil
}

// Get loads a document by id from storage.
func (s *Service) Get(ctx context.Context, id string) (document.Document, error) {
	rec, err := s.manager.Get(ctx, storage.Key(id))
	if err != nil {
		return nil, fmt.Errorf("document service: get: %w", err)
	}

	var doc document.Document
	if err := json.Unmarshal(rec.Value, &doc); err != nil {
		return nil, document.ErrInvalidJSON
	}

	return doc, nil
}

// Delete removes the document with the given id. Delete is idempotent at the storage layer.
func (s *Service) Delete(ctx context.Context, id string) error {
	doc, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	s.indexes.RemoveDocument(id, doc)
	if err := s.manager.Delete(ctx, storage.Key(id)); err != nil {
		s.indexes.IndexDocument(id, doc)
		return fmt.Errorf("document service: delete: %w", err)
	}
	return nil
}

// Update validates and replaces the document with the given id.
func (s *Service) Update(ctx context.Context, id string, doc document.Document) error {
	oldDoc, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	updated := make(document.Document, len(doc)+1)
	for field, value := range doc {
		updated[field] = value
	}
	updated["_id"] = id

	jsonBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("document service: marshal document: %w", err)
	}
	validated, err := document.Validate(jsonBytes)
	if err != nil {
		return fmt.Errorf("document service: validate document: %w", err)
	}

	rec := storage.Record{
		Key:   storage.Key(id),
		Value: storage.Value(jsonBytes),
	}
	if err := s.manager.Put(ctx, rec); err != nil {
		return fmt.Errorf("document service: update: %w", err)
	}

	s.indexes.UpdateDocument(id, oldDoc, validated)
	return nil
}

// IndexManager returns the service's secondary index manager.
func (s *Service) IndexManager() *index.IndexManager {
	return s.indexes
}

// FindByField returns documents matching an indexed field value in index order.
// Optional arguments are limit, offset, and sort field. With no arguments, all
// matching documents are returned from offset zero. A negative limit means no
// limit and is reserved for callers that need to specify an offset only.
func (s *Service) FindByField(ctx context.Context, field string, value any, options ...any) ([]document.Document, error) {
	ids := s.indexes.Lookup(field, value)
	limit, offset := -1, 0
	sortField := ""
	if len(options) > 0 {
		var ok bool
		limit, ok = options[0].(int)
		if !ok {
			return nil, fmt.Errorf("document service: invalid limit option")
		}
	}
	if len(options) > 1 {
		var ok bool
		offset, ok = options[1].(int)
		if !ok {
			return nil, fmt.Errorf("document service: invalid offset option")
		}
	}
	if len(options) > 2 {
		var ok bool
		sortField, ok = options[2].(string)
		if !ok {
			return nil, fmt.Errorf("document service: invalid sort option")
		}
	}
	if len(options) > 3 {
		return nil, fmt.Errorf("document service: invalid query options")
	}

	if sortField != "" {
		return s.findSorted(ctx, ids, limit, offset, sortField)
	}

	capacity := len(ids)
	if offset >= capacity {
		capacity = 0
	} else {
		capacity -= offset
	}
	if limit >= 0 && limit < capacity {
		capacity = limit
	}
	results := make([]document.Document, 0, capacity)
	matched := 0

	for _, id := range ids {
		doc, err := s.Get(ctx, id)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		if matched < offset {
			matched++
			continue
		}
		if limit == 0 {
			break
		}
		results = append(results, doc)
		matched++
		if limit > 0 && len(results) == limit {
			break
		}
	}

	return results, nil
}

func (s *Service) findSorted(ctx context.Context, ids []string, limit, offset int, sortField string) ([]document.Document, error) {
	results := make([]document.Document, 0, len(ids))
	for _, id := range ids {
		doc, err := s.Get(ctx, id)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		results = append(results, doc)
	}

	descending := false
	if sortField[0] == '-' {
		descending = true
		sortField = sortField[1:]
	}
	sort.SliceStable(results, func(i, j int) bool {
		return lessByField(results[i], results[j], sortField, descending)
	})

	if offset >= len(results) {
		return results[:0], nil
	}
	end := len(results)
	if limit >= 0 && offset+limit < end {
		end = offset + limit
	}
	return results[offset:end], nil
}

// AggregateByField aggregates documents matching an indexed field value.
// Sorting is applied before aggregation; pagination is intentionally ignored.
func (s *Service) AggregateByField(ctx context.Context, field string, value any, aggregate, sortField string) (map[string]any, error) {
	docs, err := s.FindByField(ctx, field, value, -1, 0, sortField)
	if err != nil {
		return nil, err
	}
	return s.AggregateDocuments(docs, aggregate)
}

// AggregateDocuments aggregates an already-filtered document result.
func (s *Service) AggregateDocuments(docs []document.Document, aggregate string) (map[string]any, error) {
	if aggregate == "count" {
		return map[string]any{"count": len(docs)}, nil
	}

	parts := strings.SplitN(aggregate, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return nil, fmt.Errorf("document service: invalid aggregate %q", aggregate)
	}
	operation, aggregateField := parts[0], parts[1]
	if operation != "sum" && operation != "avg" && operation != "min" && operation != "max" {
		return nil, fmt.Errorf("document service: invalid aggregate %q", aggregate)
	}

	values := make([]float64, 0, len(docs))
	for _, doc := range docs {
		if numericValue, ok := numericValue(doc[aggregateField]); ok {
			values = append(values, numericValue)
		}
	}
	if len(values) == 0 {
		return map[string]any{"field": aggregateField, "value": nil}, nil
	}

	switch operation {
	case "sum":
		var sum float64
		for _, numericValue := range values {
			sum += numericValue
		}
		return map[string]any{"field": aggregateField, "sum": sum}, nil
	case "avg":
		var sum float64
		for _, numericValue := range values {
			sum += numericValue
		}
		return map[string]any{"field": aggregateField, "average": sum / float64(len(values))}, nil
	case "min":
		minimum := values[0]
		for _, numericValue := range values[1:] {
			if numericValue < minimum {
				minimum = numericValue
			}
		}
		return map[string]any{"field": aggregateField, "minimum": minimum}, nil
	default:
		maximum := values[0]
		for _, numericValue := range values[1:] {
			if numericValue > maximum {
				maximum = numericValue
			}
		}
		return map[string]any{"field": aggregateField, "maximum": maximum}, nil
	}
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func lessByField(left, right document.Document, field string, descending bool) bool {
	leftValue, leftOK := left[field]
	rightValue, rightOK := right[field]
	if !leftOK || leftValue == nil {
		return false
	}
	if !rightOK || rightValue == nil {
		return true
	}

	var less, greater bool
	switch leftTyped := leftValue.(type) {
	case string:
		rightTyped, ok := rightValue.(string)
		if !ok {
			return false
		}
		less, greater = leftTyped < rightTyped, leftTyped > rightTyped
	case float64:
		rightTyped, ok := rightValue.(float64)
		if !ok {
			return false
		}
		less, greater = leftTyped < rightTyped, leftTyped > rightTyped
	case bool:
		rightTyped, ok := rightValue.(bool)
		if !ok || leftTyped == rightTyped {
			return false
		}
		less = !leftTyped && rightTyped
		greater = leftTyped && !rightTyped
	default:
		return false
	}
	if descending {
		return greater
	}
	return less
}

// FindOneByField returns the first document matching an indexed field value.
func (s *Service) FindOneByField(ctx context.Context, field string, value any) (*document.Document, error) {
	docs, err := s.FindByField(ctx, field, value)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return &docs[0], nil
}

// Exists reports whether a document with the given id is present in storage.
func (s *Service) Exists(ctx context.Context, id string) (bool, error) {
	_, err := s.manager.Get(ctx, storage.Key(id))
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("document service: exists: %w", err)
	}
	return true, nil
}
