package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

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

type adminPolicyStatement struct {
	Effect  string   `json:"effect"`
	Actions []string `json:"actions"`
	Bucket  string   `json:"bucket"`
	Prefix  string   `json:"prefix"`
}

type adminPolicy struct {
	Principal  string                 `json:"principal,omitempty"`
	Statements []adminPolicyStatement `json:"statements"`
}

type adminUserResponse struct {
	AccessKeyID string       `json:"accessKeyId"`
	Status      string       `json:"status"`
	CreatedAt   int64        `json:"createdAt"`
	UpdatedAt   int64        `json:"updatedAt"`
	Policy      *adminPolicy `json:"policy,omitempty"`
	SecretKey   string       `json:"secretKey,omitempty"`
}

type createUserRequest struct {
	AccessKeyID string      `json:"accessKeyId"`
	SecretKey   string      `json:"secretKey,omitempty"`
	Status      string      `json:"status,omitempty"`
	Policy      adminPolicy `json:"policy"`
}

type setStatusRequest struct {
	Status string `json:"status"`
}

type setPolicyRequest struct {
	Policy adminPolicy `json:"policy"`
}

type adminErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId,omitempty"`
}

type adminAPIError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

func (e *adminAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return fmt.Sprintf("admin API request failed: status=%d", e.StatusCode)
	}
	if e.RequestID == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (requestId=%s)", e.Code, e.Message, e.RequestID)
}

type adminAPIClient struct {
	baseURL   *url.URL
	region    string
	accessKey string
	secretKey string
	client    *http.Client
}

func newAdminAPIClient(opts *adminOptions, requireCredentials bool) (*adminAPIClient, error) {
	if opts == nil {
		return nil, errors.New("admin options are required")
	}
	if err := opts.resolve(requireCredentials); err != nil {
		return nil, err
	}

	baseURL, err := url.Parse(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	return &adminAPIClient{
		baseURL:   baseURL,
		region:    opts.Region,
		accessKey: opts.AccessKey,
		secretKey: opts.SecretKey,
		client: &http.Client{
			Timeout: opts.Timeout,
		},
	}, nil
}

func (c *adminAPIClient) CreateUser(ctx context.Context, request createUserRequest) (*adminUserResponse, error) {
	var out adminUserResponse
	if err := c.doJSON(ctx, http.MethodPost, "/_admin/v1/users", nil, request, &out, http.StatusCreated); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *adminAPIClient) ListUsers(ctx context.Context, limit int, cursor string) (*adminUserListResponse, error) {
	query := make(url.Values)
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if strings.TrimSpace(cursor) != "" {
		query.Set("cursor", strings.TrimSpace(cursor))
	}

	var out adminUserListResponse
	if err := c.doJSON(ctx, http.MethodGet, "/_admin/v1/users", query, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *adminAPIClient) GetUser(ctx context.Context, accessKeyID string) (*adminUserResponse, error) {
	var out adminUserResponse
	path := "/_admin/v1/users/" + url.PathEscape(strings.TrimSpace(accessKeyID))
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *adminAPIClient) DeleteUser(ctx context.Context, accessKeyID string) error {
	path := "/_admin/v1/users/" + url.PathEscape(strings.TrimSpace(accessKeyID))
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, nil, http.StatusNoContent)
}

func (c *adminAPIClient) SetUserStatus(ctx context.Context, accessKeyID, status string) (*adminUserResponse, error) {
	var out adminUserResponse
	path := "/_admin/v1/users/" + url.PathEscape(strings.TrimSpace(accessKeyID)) + "/status"
	if err := c.doJSON(ctx, http.MethodPut, path, nil, setStatusRequest{Status: status}, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *adminAPIClient) SetUserPolicy(ctx context.Context, accessKeyID string, policy adminPolicy) (*adminUserResponse, error) {
	var out adminUserResponse
	path := "/_admin/v1/users/" + url.PathEscape(strings.TrimSpace(accessKeyID)) + "/policy"
	if err := c.doJSON(ctx, http.MethodPut, path, nil, setPolicyRequest{Policy: policy}, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *adminAPIClient) Health(ctx context.Context) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/healthz", nil, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	text := strings.TrimSpace(string(body))
	if resp.StatusCode != http.StatusOK {
		if text == "" {
			text = http.StatusText(resp.StatusCode)
		}
		return text, fmt.Errorf("health check failed: status=%d", resp.StatusCode)
	}
	if text == "" {
		text = "ok"
	}
	return text, nil
}

func (c *adminAPIClient) doJSON(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body any,
	out any,
	expectedStatus int,
) error {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}

	req, err := c.newRequest(ctx, method, path, query, payload)
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if err := signSigV4Request(req, payload, c.accessKey, c.secretKey, c.region, "s3"); err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != expectedStatus {
		apiErr := &adminAPIError{StatusCode: resp.StatusCode}
		parsed := adminErrorResponse{}
		if len(raw) > 0 && json.Unmarshal(raw, &parsed) == nil {
			apiErr.Code = parsed.Code
			apiErr.Message = parsed.Message
			apiErr.RequestID = parsed.RequestID
		}
		if apiErr.Message == "" {
			apiErr.Message = strings.TrimSpace(string(raw))
		}
		return apiErr
	}

	if out == nil || len(raw) == 0 {
		return nil
	}
	return json.NewDecoder(bytes.NewReader(raw)).Decode(out)
}

func (c *adminAPIClient) newRequest(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	payload []byte,
) (*http.Request, error) {
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	u.RawQuery = ""
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	return req, nil
}
