package auth

import "context"

type RequestContext struct {
	Authenticated bool
	AccessKeyID   string
	AuthType      string
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
