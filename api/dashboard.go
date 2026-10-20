package api

import (
	"context"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/webui"
	"strings"
	"time"
)

// EnableDashboard mounts the read-only dashboard at /ui. It is opt-in: an empty
// token leaves it disabled, and the JSON API requires the token as a bearer
// value. The page itself carries no data.
func (h *Handler) EnableDashboard(token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	dashboard, err := webui.NewHandler(webui.Config{BasePath: "/ui", Token: token}, &dashboardProvider{svc: h.svc})
	if err != nil {
		h.logger.Warn("dashboard_disabled", "error", err)
		return
	}
	h.router.Handle("/ui", dashboard)
	h.router.Handle("/ui/*", dashboard)
	h.logger.Info("dashboard_enabled", "path", "/ui")
}

type dashboardProvider struct {
	svc *service.ObjectService
}

func (p *dashboardProvider) Summary(ctx context.Context) (webui.Summary, error) {
	buckets, err := p.svc.ListBuckets()
	if err != nil {
		return webui.Summary{}, err
	}
	summary := webui.Summary{Buckets: int64(len(buckets))}
	for _, bucket := range buckets {
		count, logical, err := p.bucketStats(ctx, bucket)
		if err != nil {
			return webui.Summary{}, err
		}
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
		count, logical, err := p.bucketStats(ctx, name)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, webui.Bucket{Name: name, Objects: count, Bytes: logical})
	}
	return buckets, nil
}

func (p *dashboardProvider) Objects(ctx context.Context, bucket, prefix string) ([]webui.Object, error) {
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
