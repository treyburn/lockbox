FROM golang:1.25.1-alpine AS base

# Install go dependencies
WORKDIR /src
COPY ./go.mod ./go.sum ./
RUN go mod download

# Install the sources
COPY ./cmd /src/cmd
COPY ./internal /src/internal

ENV CGO_ENABLED=0

CMD ["go", "run", "./cmd/..."]