# Cross-compiles on the build machine's own architecture (no emulation).
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.26-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY web ./web
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=
ARG BUILD_DATE=
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -tags timetzdata \
      -ldflags="-s -w -X main.version=$VERSION -X main.revision=$REVISION -X main.buildDate=$BUILD_DATE" \
      -o /baldrick-monitor .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /baldrick-monitor /baldrick-monitor
ARG VERSION=dev
ARG REVISION=
LABEL org.opencontainers.image.title="baldrick-monitor" \
      org.opencontainers.image.description="Monitors Baldrick pixel controllers" \
      org.opencontainers.image.source="https://github.com/terensapp/baldrick-monitor" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION
EXPOSE 8420
VOLUME /var/lib/baldrick-monitor
ENTRYPOINT ["/baldrick-monitor", "-config", "/etc/baldrick-monitor/config.json"]
