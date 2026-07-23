package document

import "encoding/json"

// Validate parses and validates a JSON document payload.
// The root value must be a non-empty JSON object.
func Validate(data []byte) (Document, error) {
	if len(data) == 0 {
		return nil, ErrEmptyDocument
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, ErrInvalidJSON
	}
	if len(doc) == 0 {
		return nil, ErrEmptyDocument
	}

	return doc, nil
}
