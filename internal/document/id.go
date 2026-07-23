package document

import (
	"github.com/oklog/ulid/v2"
)

// NewID returns a new lexicographically sortable document identifier.
func NewID() string {
	return ulid.Make().String()
}
