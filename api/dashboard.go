package api

import (
	"context"
	"fs/auth"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/webui"
	"net/http"
	"strings"
	"time"
)

// EnableDashboard mounts the read-only dashboard at /ui. With auth enabled it
// reuses the S3 account system over HTTP Basic (the browser prompts for an
// access key and secret); otherwise an explicit token enables a static bearer
// guard. An empty token with auth disabled leaves the dashboard off.
func (h *Handler) EnableDashboard(token string) {
	config := webui.Config{BasePath: "/ui"}
	switch {
	case h.authSvc != nil && h.authSvc.Config().Enabled:
		config.Authorize = h.dashboardBasicAuth
	case strings.TrimSpace(token) != "":
		config.Token = strings.TrimSpace(token)
	default:
		return
	}
	h.dashboardEnabled = true

	dashboard, err := webui.NewHandler(config, &dashboardProvider{svc: h.svc, auth: h.authSvc})
	if err != nil {
		h.logger.Warn("dashboard_disabled", "error", err)
		return
	}
	h.router.Handle("/ui", dashboard)
	h.router.Handle("/ui/*", dashboard)
	h.logger.Info("dashboard_enabled", "path", "/ui", "auth", h.authSvc != nil && h.authSvc.Config().Enabled)
}

// dashboardRequest lets the dashboard's own authentication replace SigV4 for
// /ui; the S3 middleware would otherwise reject unsigned browser requests.
func (h *Handler) dashboardRequest(r *http.Request) bool {
	if !h.dashboardEnabled {
		return false
	}
	return r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/")
}

type dashboardViewerKey struct{}

func (h *Handler) dashboardBasicAuth(r *http.Request) (*http.Request, bool) {
	accessKey, secret, ok := r.BasicAuth()
	if !ok {
		return r, false
	}
	if err := h.authSvc.AuthenticateCredentials(accessKey, secret); err != nil {
		return r, false
	}
	return r.WithContext(context.WithValue(r.Context(), dashboardViewerKey{}, accessKey)), true
}

type dashboardProvider struct {
	svc  *service.ObjectService
	auth *auth.Service
}

func dashboardViewer(ctx context.Context) string {
	viewer, _ := ctx.Value(dashboardViewerKey{}).(string)
	return viewer
}

// allowed reports whether the logged-in account may list the bucket/prefix.
func (p *dashboardProvider) allowed(ctx context.Context, bucket, prefix string) bool {
	viewer := dashboardViewer(ctx)
	if viewer == "" || p.auth == nil {
		return true
	}
	return p.auth.Authorize(viewer, auth.RequestTarget{
		Action: auth.ActionListBucket,
		Bucket: bucket,
		Prefix: prefix,
	}) == nil
}

func (p *dashboardProvider) Summary(ctx context.Context) (webui.Summary, error) {
	buckets, err := p.svc.ListBuckets()
	if err != nil {
		return webui.Summary{}, err
	}
	var summary webui.Summary
	for _, bucket := range buckets {
		if !p.allowed(ctx, bucket, "") {
			continue
		}
		count, logical, err := p.bucketStats(ctx, bucket)
		if err != nil {
			return webui.Summary{}, err
		}
		summary.Buckets++
		summary.Objects += count
		summary.LogicalBytes += logical
	}
	summary.PhysicalBytes, err = p.svc.DiskUsage()
	if err != nil {
		return webui.Summary{}, err
	}
	return summary, nil
}

func (p *dashboardProvider) Buckets(ctx context.Context) ([]webui.Bucket, error) {
	names, err := p.svc.ListBuckets()
	if err != nil {
		return nil, err
	}
	buckets := make([]webui.Bucket, 0, len(names))
	for _, name := range names {
		if !p.allowed(ctx, name, "") {
			continue
		}
		count, logical, err := p.bucketStats(ctx, name)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, webui.Bucket{Name: name, Objects: count, Bytes: logical})
	}
	return buckets, nil
}

func (p *dashboardProvider) Objects(ctx context.Context, bucket, prefix string) ([]webui.Object, error) {
	if !p.allowed(ctx, bucket, prefix) {
		return []webui.Object{}, nil
	}
	const limit = 200
	objects := make([]webui.Object, 0, limit)
	err := p.svc.ForEachObjectFrom(bucket, prefix, func(manifest *models.ObjectManifest) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		objects = append(objects, webui.Object{
			Key:          manifest.Key,
			Size:         manifest.Size,
			ETag:         manifest.ETag,
			LastModified: time.Unix(manifest.CreatedAt, 0).UTC().Format(time.RFC3339),
		})
		if len(objects) >= limit {
			return metadata.StopIteration()
		}
		return nil
	})
	if err != nil && !metadata.IsIterationStopped(err) {
		return nil, err
	}
	return objects, nil
}

func (p *dashboardProvider) bucketStats(ctx context.Context, bucket string) (int64, int64, error) {
	var count, logical int64
	err := p.svc.ForEachObjectFrom(bucket, "", func(manifest *models.ObjectManifest) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		count++
		logical += manifest.Size
		return nil
	})
	return count, logical, err
}
