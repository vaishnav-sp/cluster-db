// Package document defines the document layer for ClusterDB.
// Documents are JSON objects that will sit logically above the KV storage engine.
package document

// Document is a JSON object represented as a string-keyed map.
// Field values use encoding/json decoding rules (for example, numbers decode as float64).
type Document map[string]any
