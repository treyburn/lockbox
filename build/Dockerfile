FROM golang:1.25.1-alpine AS base

# CGo is not Go
ENV CGO_ENABLED=0

# Install go dependencies
WORKDIR /src
COPY ./go.mod ./go.sum ./
RUN go mod download

# Install the sources
COPY ./cmd /src/cmd
COPY ./internal /src/internal

CMD ["go", "run", "./cmd/..."]

FROM base AS test

# CGo must be enabled for the -race flag
ENV CGO_ENABLED=1

# need to pull in gcc for -race flag
RUN apk add build-base

CMD ["go", "test", "-race", "./..."]