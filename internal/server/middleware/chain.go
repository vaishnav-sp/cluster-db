package middleware

import "net/http"

// Chain applies middlewares to the provided handler in the correct order.
func Chain(handler http.Handler, middlewares ...Middleware) http.Handler {
	if handler == nil {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] != nil {
			handler = middlewares[i](handler)
		}
	}

	return handler
}
