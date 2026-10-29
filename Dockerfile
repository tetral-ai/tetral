FROM docker.io/library/golang:1.26.10-alpine@sha256:27f2d3b83cfc5803c8078f8e730af0f7c431bf067afaec8f60a36fb763187cba AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# One image serves every Go service; the deployment manifests pick a binary per
# container. The list is spelled out rather than globbed so the image contents
# stay a declared set — a new cmd/ directory does not silently ship.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ \
      ./services/api/cmd/tetral-api \
      ./cmd/tetral-db-prepare \
      ./services/auth/cmd/tetral-auth \
      ./services/auth/cmd/tetral-auth-policy \
      ./cmd/tetral-bootstrap \
      ./services/bridge/cmd/bridge-api \
      ./services/job-runner/cmd/job-runner \
      ./services/cleanup/cmd/tetral-cleanup \
      ./services/event-stream/cmd/event-stream \
      ./services/computer-hub/cmd/tetral-computer-hub \
      ./services/git-proxy/cmd/git-proxy \
      ./services/queue/cmd/tetral-queue \
      ./services/sandbox/cmd/tetral-sandbox \
      ./services/web-connector/cmd/web-connector

# Static binaries need no distribution: this base carries CA certificates for
# outbound TLS, a numeric non-root user for runAsNonRoot, and nothing else —
# no shell, no package manager. The services never shell out, so nothing is
# missing.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=build /out/ /usr/local/bin/

# No default command. Every manifest names the binary it wants, and a default
# here would be one service pretending to be the image's purpose.
