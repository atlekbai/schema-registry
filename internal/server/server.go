package server

import (
	"net/http"

	"connectrpc.com/connect"
)

// ConnectService is implemented by each service to register its connect handler.
type ConnectService interface {
	RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler)
}
