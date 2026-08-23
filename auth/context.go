package auth

import "context"

type RequestContext struct {
	Authenticated bool
	// Streaming carries chunk-signature material for STREAMING-AWS4-HMAC-SHA256*
	// request bodies; nil unless auth verified such an upload.
	Streaming   *StreamingAuth
	AccessKeyID string
	AuthType    string
}

type contextKey int

const requestContextKey contextKey = iota

func WithRequestContext(ctx context.Context, authCtx RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey, authCtx)
}

func GetRequestContext(ctx context.Context) (RequestContext, bool) {
	value := ctx.Value(requestContextKey)
	authCtx, ok := value.(RequestContext)
	return authCtx, ok
}
