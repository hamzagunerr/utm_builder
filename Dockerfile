# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Bağımlılıkları kopyala ve indir
COPY go.mod go.sum ./
RUN go mod download

# Kaynak kodu kopyala ve derle
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o utm-builder-bot .

# Runtime stage - minimal image
FROM alpine:3.19

# CA sertifikaları (HTTPS için gerekli)
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Binary'yi kopyala
COPY --from=builder /app/utm-builder-bot .

# Non-root user
RUN adduser -D -g '' appuser
USER appuser

# Bot'u çalıştır
CMD ["./utm-builder-bot"]
