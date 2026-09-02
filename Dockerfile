# Build the proxy as a static binary, then ship it in a distroless image.
FROM golang:1.26.5 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=docker
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/axiom-tempo-proxy ./cmd/axiom-tempo-proxy

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/axiom-tempo-proxy /usr/local/bin/axiom-tempo-proxy
EXPOSE 3200
ENTRYPOINT ["/usr/local/bin/axiom-tempo-proxy"]
