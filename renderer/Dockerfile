FROM golang:latest AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/bin/renderer main.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y chromium ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/bin/renderer .
CMD ["./renderer"]
