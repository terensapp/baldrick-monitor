FROM docker.io/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY web ./web
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /baldrick-monitor .

FROM scratch
COPY --from=build /baldrick-monitor /baldrick-monitor
EXPOSE 8420
VOLUME /var/lib/baldrick-monitor
ENTRYPOINT ["/baldrick-monitor", "-config", "/etc/baldrick-monitor/config.json"]
