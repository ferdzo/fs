FROM golang:1.25-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
  -trimpath \
  -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
  -o /app/fs .

FROM alpine:3.23 AS runner

# Non-root runtime user; /data is the default DATA_PATH and the declared volume.
RUN addgroup -g 10001 fs \
 && adduser -D -u 10001 -G fs -H -h /data fs \
 && mkdir -p /data \
 && chown fs:fs /data

COPY --from=build /app/fs /app/fs

USER fs
ENV DATA_PATH=/data
VOLUME ["/data"]

EXPOSE 2600
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD wget -q -O /dev/null "http://127.0.0.1:${PORT:-2600}/healthz" || exit 1
CMD ["/app/fs"]
