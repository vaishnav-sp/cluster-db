// Package index provides in-memory secondary indexes over document fields.
package index

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/vaishnav-sp/cluster-db/internal/document"
)

// IndexManager maintains inverted indexes from field values to document IDs.
type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]map[string]map[string]struct{}
}

// NewIndexManager creates an empty IndexManager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]map[string]map[string]struct{}),
	}
}

// IndexDocument adds document id to the indexes for each indexable top-level field.
func (m *IndexManager) IndexDocument(id string, doc document.Document) {
	fieldValues := indexableFieldValues(doc)

	m.mu.Lock()
	defer m.mu.Unlock()

	for field, value := range fieldValues {
		m.addIDLocked(field, value, id)
	}
}

// RemoveDocument removes document id from the indexes for each indexable top-level field in doc.
func (m *IndexManager) RemoveDocument(id string, doc document.Document) {
	fieldValues := indexableFieldValues(doc)

	m.mu.Lock()
	defer m.mu.Unlock()

	for field, value := range fieldValues {
		m.removeIDLocked(field, value, id)
	}
}

// UpdateDocument reindexes a document after a field change.
func (m *IndexManager) UpdateDocument(id string, oldDoc, newDoc document.Document) {
	oldFields := indexableFieldValues(oldDoc)
	newFields := indexableFieldValues(newDoc)

	m.mu.Lock()
	defer m.mu.Unlock()

	for field, value := range oldFields {
		m.removeIDLocked(field, value, id)
	}
	for field, value := range newFields {
		m.addIDLocked(field, value, id)
	}
}

// Lookup returns document IDs whose indexed value equals fmt.Sprint(value) for field.
func (m *IndexManager) Lookup(field string, value any) []string {
	key := fmt.Sprint(value)

	m.mu.RLock()
	defer m.mu.RUnlock()

	fieldIndex := m.indexes[field]
	if fieldIndex == nil {
		return nil
	}

	ids := fieldIndex[key]
	if len(ids) == 0 {
		return nil
	}

	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Fields returns the names of all fields that currently have at least one indexed value.
func (m *IndexManager) Fields() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fields := make([]string, 0, len(m.indexes))
	for field := range m.indexes {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func (m *IndexManager) addIDLocked(field, value, id string) {
	fieldIndex, ok := m.indexes[field]
	if !ok {
		fieldIndex = make(map[string]map[string]struct{})
		m.indexes[field] = fieldIndex
	}

	idSet, ok := fieldIndex[value]
	if !ok {
		idSet = make(map[string]struct{})
		fieldIndex[value] = idSet
	}

	idSet[id] = struct{}{}
}

func (m *IndexManager) removeIDLocked(field, value, id string) {
	fieldIndex := m.indexes[field]
	if fieldIndex == nil {
		return
	}

	idSet := fieldIndex[value]
	if idSet == nil {
		return
	}

	delete(idSet, id)
	if len(idSet) > 0 {
		return
	}

	delete(fieldIndex, value)
	if len(fieldIndex) > 0 {
		return
	}

	delete(m.indexes, field)
}

func indexableFieldValues(doc document.Document) map[string]string {
	out := make(map[string]string)
	for field, value := range doc {
		if field == "_id" {
			continue
		}
		if isNestedValue(value) {
			continue
		}
		out[field] = fmt.Sprint(value)
	}
	return out
}

func isNestedValue(value any) bool {
	if value == nil {
		return false
	}

	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Map || kind == reflect.Slice || kind == reflect.Array
}
