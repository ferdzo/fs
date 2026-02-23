FROM golang:1.25-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/fs .

FROM scratch AS runner

COPY --from=build /app/fs /app/fs

WORKDIR /app
CMD ["/app/fs"]
