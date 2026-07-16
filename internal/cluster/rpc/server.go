package rpc

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// HandlerFunc is a placeholder callback signature used by the server handlers.
type HandlerFunc func(req any) (any, error)

// Server exposes the cluster RPC HTTP endpoints.
type Server struct {
	HeartbeatHandler func(HeartbeatRequest) (HeartbeatResponse, error)
	JoinHandler      func(JoinRequest) (JoinResponse, error)
	LeaveHandler     func(LeaveRequest) (LeaveResponse, error)
	AppendHandler    func(AppendEntriesRequest) (AppendEntriesResponse, error)
	KVGetHandler     func(KVGetRequest) (KVGetResponse, error)
	KVPutHandler     func(KVPutRequest) (KVPutResponse, error)
	KVDeleteHandler  func(KVDeleteRequest) (KVDeleteResponse, error)
	ReplicaPutHandler    func(ReplicaPutRequest) (ReplicaPutResponse, error)
	ReplicaDeleteHandler func(ReplicaDeleteRequest) (ReplicaDeleteResponse, error)
}

// NewServer creates a new RPC server with no-op handlers by default.
func NewServer() *Server {
	return &Server{}
}

// Handler returns an http.Handler that exposes the cluster RPC endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/join", s.handleJoin)
	mux.HandleFunc("/cluster/leave", s.handleLeave)
	mux.HandleFunc("/cluster/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/cluster/append", s.handleAppend)
	mux.HandleFunc("/cluster/kv/get", s.handleKVGet)
	mux.HandleFunc("/cluster/kv/put", s.handleKVPut)
	mux.HandleFunc("/cluster/kv/delete", s.handleKVDelete)
	mux.HandleFunc("/cluster/replica/put", s.handleReplicaPut)
	mux.HandleFunc("/cluster/replica/delete", s.handleReplicaDelete)
	return mux
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, HeartbeatResponse{Message: "method not allowed"})
		return
	}
	var req HeartbeatRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, HeartbeatResponse{Message: err.Error()})
		return
	}
	if err := validateHeartbeatRequest(req); err != nil {
		writeJSON(w, http.StatusBadRequest, HeartbeatResponse{Message: err.Error()})
		return
	}
	if s.HeartbeatHandler == nil {
		writeJSON(w, http.StatusOK, HeartbeatResponse{Accepted: true, Message: "ok"})
		return
	}
	resp, err := s.HeartbeatHandler(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, HeartbeatResponse{Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, JoinResponse{Message: "method not allowed"})
		return
	}
	var req JoinRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, JoinResponse{Message: err.Error()})
		return
	}
	if err := validateJoinRequest(req); err != nil {
		writeJSON(w, http.StatusBadRequest, JoinResponse{Message: err.Error()})
		return
	}
	if s.JoinHandler == nil {
		writeJSON(w, http.StatusOK, JoinResponse{Accepted: true, Message: "ok"})
		return
	}
	resp, err := s.JoinHandler(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, JoinResponse{Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, LeaveResponse{Message: "method not allowed"})
		return
	}
	var req LeaveRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, LeaveResponse{Message: err.Error()})
		return
	}
	if err := validateLeaveRequest(req); err != nil {
		writeJSON(w, http.StatusBadRequest, LeaveResponse{Message: err.Error()})
		return
	}
	if s.LeaveHandler == nil {
		writeJSON(w, http.StatusOK, LeaveResponse{Accepted: true, Message: "ok"})
		return
	}
	resp, err := s.LeaveHandler(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, LeaveResponse{Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, AppendEntriesResponse{Message: "method not allowed"})
		return
	}
	var req AppendEntriesRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, AppendEntriesResponse{Message: err.Error()})
		return
	}
	if err := validateAppendRequest(req); err != nil {
		writeJSON(w, http.StatusBadRequest, AppendEntriesResponse{Message: err.Error()})
		return
	}
	if s.AppendHandler == nil {
		writeJSON(w, http.StatusOK, AppendEntriesResponse{Accepted: true, Message: "ok"})
		return
	}
	resp, err := s.AppendHandler(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, AppendEntriesResponse{Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func validateHeartbeatRequest(req HeartbeatRequest) error {
	if req.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	return nil
}

func validateJoinRequest(req JoinRequest) error {
	if req.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if req.Address == "" {
		return fmt.Errorf("address is required")
	}
	return nil
}

func validateLeaveRequest(req LeaveRequest) error {
	if req.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	return nil
}

func validateAppendRequest(req AppendEntriesRequest) error {
	if req.LeaderID == "" {
		return fmt.Errorf("leader_id is required")
	}
	return nil
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, KVGetResponse{Error: "method not allowed"})
		return
	}
	var req KVGetRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, KVGetResponse{Error: err.Error()})
		return
	}
	if s.KVGetHandler == nil {
		writeJSON(w, http.StatusNotImplemented, KVGetResponse{Error: "handler not implemented"})
		return
	}
	resp, err := s.KVGetHandler(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, KVGetResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, KVPutResponse{Error: "method not allowed"})
		return
	}
	var req KVPutRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, KVPutResponse{Error: err.Error()})
		return
	}
	if s.KVPutHandler == nil {
		writeJSON(w, http.StatusNotImplemented, KVPutResponse{Error: "handler not implemented"})
		return
	}
	resp, err := s.KVPutHandler(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, KVPutResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, KVDeleteResponse{Error: "method not allowed"})
		return
	}
	var req KVDeleteRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, KVDeleteResponse{Error: err.Error()})
		return
	}
	if s.KVDeleteHandler == nil {
		writeJSON(w, http.StatusNotImplemented, KVDeleteResponse{Error: "handler not implemented"})
		return
	}
	resp, err := s.KVDeleteHandler(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, KVDeleteResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleReplicaPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ReplicaPutResponse{Error: "method not allowed"})
		return
	}
	var req ReplicaPutRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ReplicaPutResponse{Error: err.Error()})
		return
	}
	if s.ReplicaPutHandler == nil {
		writeJSON(w, http.StatusNotImplemented, ReplicaPutResponse{Error: "handler not implemented"})
		return
	}
	resp, err := s.ReplicaPutHandler(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ReplicaPutResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleReplicaDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ReplicaDeleteResponse{Error: "method not allowed"})
		return
	}
	var req ReplicaDeleteRequest
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ReplicaDeleteResponse{Error: err.Error()})
		return
	}
	if s.ReplicaDeleteHandler == nil {
		writeJSON(w, http.StatusNotImplemented, ReplicaDeleteResponse{Error: "handler not implemented"})
		return
	}
	resp, err := s.ReplicaDeleteHandler(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ReplicaDeleteResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}


