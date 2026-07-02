# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.24
ARG ALPINE_VERSION=3.21
ARG BUF_VERSION=v1.50.1

FROM golang:${GO_VERSION}-alpine AS builder

ARG BUF_VERSION

RUN apk add --no-cache ca-certificates git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

RUN GOBIN=/usr/local/bin go install github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}

COPY . ./

# Validate protobuf definitions and generate Go gRPC client/server stubs into
# /src/gen. Generated stubs are intentionally not committed to the repository.
# To inspect or extract them from the builder stage:
#   docker build --target builder -t mycel-builder .
#   cid=$(docker create mycel-builder)
#   docker cp "$cid":/src/gen ./gen
#   docker rm "$cid"
RUN buf lint && buf generate

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mycel ./cmd/mycel

FROM alpine:${ALPINE_VERSION}

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /out/mycel /usr/local/bin/mycel

ENV MYCELDB_DATA_DIR=/data/mycel

VOLUME ["/data/mycel"]

ENTRYPOINT ["mycel"]
CMD ["--help"]
