FROM golang:1.25-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/fs .

FROM alpine:3.23 AS runner

COPY --from=build /app/fs /app/fs

WORKDIR /app
EXPOSE 2600
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD wget -q -O /dev/null "http://127.0.0.1:${PORT:-2600}/healthz" || exit 1
CMD ["/app/fs"]
