// Package service bridges document operations to the KV storage manager.
// Each document is stored as a JSON blob keyed by its ULID identifier.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vaishnav-sp/cluster-db/internal/document"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// Service persists and retrieves documents through the storage manager.
type Service struct {
	manager *manager.Manager
}

// New constructs a document Service backed by the given storage manager.
func New(mgr *manager.Manager) *Service {
	return &Service{manager: mgr}
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
	if err := s.manager.Delete(ctx, storage.Key(id)); err != nil {
		return fmt.Errorf("document service: delete: %w", err)
	}
	return nil
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
