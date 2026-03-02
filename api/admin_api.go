package api

import (
	"encoding/json"
	"errors"
	"fs/auth"
	"fs/models"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const (
	maxAdminJSONBodyBytes = 1 << 20
	defaultAdminPageSize  = 100
	maxAdminPageSize      = 1000
)

type adminErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId,omitempty"`
}

type adminCreateUserRequest struct {
	AccessKeyID string            `json:"accessKeyId"`
	SecretKey   string            `json:"secretKey,omitempty"`
	Status      string            `json:"status,omitempty"`
	Policy      models.AuthPolicy `json:"policy"`
}

type adminUserListItem struct {
	AccessKeyID string `json:"accessKeyId"`
	Status      string `json:"status"`
	CreatedAt   int64  `json:"createdAt"`
	UpdatedAt   int64  `json:"updatedAt"`
}

type adminUserListResponse struct {
	Items      []adminUserListItem `json:"items"`
	NextCursor string              `json:"nextCursor,omitempty"`
}

type adminUserResponse struct {
	AccessKeyID string             `json:"accessKeyId"`
	Status      string             `json:"status"`
	CreatedAt   int64              `json:"createdAt"`
	UpdatedAt   int64              `json:"updatedAt"`
	Policy      *models.AuthPolicy `json:"policy,omitempty"`
	SecretKey   string             `json:"secretKey,omitempty"`
}

func (h *Handler) registerAdminRoutes() {
	h.router.Route("/_admin/v1", func(r chi.Router) {
		r.Post("/users", h.handleAdminCreateUser)
		r.Get("/users", h.handleAdminListUsers)
		r.Get("/users/{accessKeyId}", h.handleAdminGetUser)
		r.Delete("/users/{accessKeyId}", h.handleAdminDeleteUser)
	})
}

func (h *Handler) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if !h.requireBootstrapAdmin(w, r) {
		return
	}

	var req adminCreateUserRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeAdminError(w, r, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	created, err := h.authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: req.AccessKeyID,
		SecretKey:   req.SecretKey,
		Status:      req.Status,
		Policy:      req.Policy,
	})
	if err != nil {
		writeMappedAdminError(w, r, err)
		return
	}

	resp := adminUserResponse{
		AccessKeyID: created.AccessKeyID,
		Status:      created.Status,
		CreatedAt:   created.CreatedAt,
		UpdatedAt:   created.UpdatedAt,
		Policy:      &created.Policy,
		SecretKey:   created.SecretKey,
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) handleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	if !h.requireBootstrapAdmin(w, r) {
		return
	}

	limit := defaultAdminPageSize
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxAdminPageSize {
			writeAdminError(w, r, http.StatusBadRequest, "InvalidRequest", "limit must be between 1 and 1000")
			return
		}
		limit = parsed
	}
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))

	users, nextCursor, err := h.authSvc.ListUsers(limit, cursor)
	if err != nil {
		writeMappedAdminError(w, r, err)
		return
	}

	items := make([]adminUserListItem, 0, len(users))
	for _, user := range users {
		items = append(items, adminUserListItem{
			AccessKeyID: user.AccessKeyID,
			Status:      user.Status,
			CreatedAt:   user.CreatedAt,
			UpdatedAt:   user.UpdatedAt,
		})
	}

	writeJSON(w, http.StatusOK, adminUserListResponse{
		Items:      items,
		NextCursor: nextCursor,
	})
}

func (h *Handler) handleAdminGetUser(w http.ResponseWriter, r *http.Request) {
	if !h.requireBootstrapAdmin(w, r) {
		return
	}

	accessKeyID := chi.URLParam(r, "accessKeyId")
	user, err := h.authSvc.GetUser(accessKeyID)
	if err != nil {
		writeMappedAdminError(w, r, err)
		return
	}

	resp := adminUserResponse{
		AccessKeyID: user.AccessKeyID,
		Status:      user.Status,
		CreatedAt:   user.CreatedAt,
		UpdatedAt:   user.UpdatedAt,
		Policy:      &user.Policy,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	if !h.requireBootstrapAdmin(w, r) {
		return
	}
	accessKeyID := chi.URLParam(r, "accessKeyId")
	if err := h.authSvc.DeleteUser(accessKeyID); err != nil {
		writeMappedAdminError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) requireBootstrapAdmin(w http.ResponseWriter, r *http.Request) bool {
	authCtx, ok := auth.GetRequestContext(r.Context())
	if !ok || !authCtx.Authenticated {
		writeAdminError(w, r, http.StatusForbidden, "Forbidden", "admin credentials are required")
		return false
	}
	if h.authSvc == nil {
		writeAdminError(w, r, http.StatusForbidden, "Forbidden", "admin access is not configured")
		return false
	}

	bootstrap := strings.TrimSpace(h.authSvc.Config().BootstrapAccessKey)
	if bootstrap == "" || authCtx.AccessKeyID != bootstrap {
		writeAdminError(w, r, http.StatusForbidden, "Forbidden", "admin access denied")
		return false
	}
	return true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func writeMappedAdminError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrInvalidUserInput):
		writeAdminError(w, r, http.StatusBadRequest, "InvalidRequest", err.Error())
	case errors.Is(err, auth.ErrUserAlreadyExists):
		writeAdminError(w, r, http.StatusConflict, "UserAlreadyExists", "user already exists")
	case errors.Is(err, auth.ErrUserNotFound):
		writeAdminError(w, r, http.StatusNotFound, "UserNotFound", "user was not found")
	case errors.Is(err, auth.ErrAuthNotEnabled):
		writeAdminError(w, r, http.StatusServiceUnavailable, "AuthDisabled", "authentication subsystem is disabled")
	default:
		writeAdminError(w, r, http.StatusInternalServerError, "InternalError", "internal server error")
	}
}

func writeAdminError(w http.ResponseWriter, r *http.Request, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	requestID := middleware.GetReqID(r.Context())
	if requestID != "" {
		w.Header().Set("x-amz-request-id", requestID)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(adminErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
