FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/bench ./cmd/bench

FROM alpine:3.19
COPY --from=builder /bin/bench /bin/bench
ENTRYPOINT ["/bin/bench"]
