# syntax=docker/dockerfile:1.7

# Build from the myceldb directory so the local mycel-api protobuf contracts are
# available to the mycel module:
#   docker build -f mycel/Dockerfile -t local/myceld:dev .

ARG GO_VERSION=1.25.5
ARG ALPINE_VERSION=3.21

FROM golang:${GO_VERSION}-alpine AS builder

RUN apk add --no-cache bash ca-certificates curl git make openjdk17-jre

WORKDIR /src

COPY mycel/go.mod mycel/go.sum ./mycel/

WORKDIR /src/mycel
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY mycel-api ../mycel-api
COPY mycel ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    ./scripts/generate-proto.sh && \
    make generate-gql-parser && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/myceld ./cmd/myceld && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mycel ./cmd/mycel

FROM alpine:${ALPINE_VERSION}

RUN apk add --no-cache ca-certificates tzdata busybox-extras

WORKDIR /app
COPY --from=builder /out/myceld /usr/local/bin/myceld
COPY --from=builder /out/mycel /usr/local/bin/mycel

ENV MYCELD_DATA_DIR=/data/mycel \
    MYCELD_GRPC_ADDR=0.0.0.0:9091

VOLUME ["/data/mycel"]
EXPOSE 9091

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD nc -z 127.0.0.1 9091 || exit 1

ENTRYPOINT ["myceld"]
