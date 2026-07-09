package middleware

import "net/http"

// Middleware is a reusable HTTP middleware function.
type Middleware func(http.Handler) http.Handler
