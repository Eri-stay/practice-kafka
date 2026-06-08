# Stage 1: build application
FROM golang:1.22-alpine AS builder
WORKDIR /app

#cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY cmd cmd
COPY db/ ./db/
COPY config/ ./config/
COPY entities/ ./entities/
COPY messenger/ ./messenger/
COPY pkg/ ./pkg/

RUN CGO_ENABLED=0 GOOS=linux build -o email-service .


# Stage 2: create final image
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/email-services .

ENTRYPOINT ["./email-service"]
