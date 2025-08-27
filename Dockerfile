# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files (if they exist)
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o toydb main.go

# Final stage
FROM alpine:latest

# Install necessary packages for a minimal runtime
RUN apk --no-cache add ca-certificates

# Set working directory
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/toydb .

# Expose any necessary ports (if applicable)
# EXPOSE 8080

# Set the entry point
ENTRYPOINT ["./toydb"]
