# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.26.7-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -buildid=' -o /responses-ws-sse-bridge .
RUN mkdir -m 0700 /spool

FROM scratch
COPY --from=builder /responses-ws-sse-bridge /responses-ws-sse-bridge
# Private (0700) spool dir owned by the non-root runtime user. Production
# mounts a disk-backed named volume here (D3: tmpfs/ramfs refused at start).
COPY --from=builder --chown=65534:65534 /spool /spool
ENV SPOOL_DIR=/spool
USER 65534:65534
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["/responses-ws-sse-bridge", "healthcheck"]
ENTRYPOINT ["/responses-ws-sse-bridge"]
