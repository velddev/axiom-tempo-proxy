#!/usr/bin/env bash
# Deploy axiom-tempo-proxy to a GCP Compute Engine instance as a systemd
# service listening on 127.0.0.1:3200 (Grafana on the same box reaches it
# at http://localhost:3200/<dataset>).
#
# Usage:
#   AXIOM_TOKEN=... deploy/deploy.sh <instance> <zone> [listen-addr]
#
# Requires gcloud with SSH access to the instance. The token is written to
# /etc/axiom-tempo-proxy/env (mode 600) on the instance; it is passed via
# an environment variable on the remote command line and never stored in
# the repo.
set -euo pipefail

INSTANCE="${1:?instance name}"
ZONE="${2:?zone}"
LISTEN="${3:-127.0.0.1:3200}"
: "${AXIOM_TOKEN:?AXIOM_TOKEN must be set}"
ALLOWED="${PROXY_ALLOWED_DATASETS:-}"
DEFAULT_DATASET="${AXIOM_DATASET:-}"

cd "$(dirname "$0")/.."

ARCH=$(gcloud compute ssh "$INSTANCE" --zone "$ZONE" --command 'uname -m' | tr -d '\r')
case "$ARCH" in
  x86_64) GOARCH=amd64 ;;
  aarch64) GOARCH=arm64 ;;
  *) echo "unsupported arch $ARCH" >&2; exit 1 ;;
esac

VERSION=$(git rev-parse --short HEAD 2>/dev/null || echo dev)
echo "building linux/$GOARCH ($VERSION)"
GOOS=linux GOARCH=$GOARCH CGO_ENABLED=0 go build -trimpath \
  -ldflags="-s -w -X main.version=$VERSION" \
  -o "bin/axiom-tempo-proxy-linux-$GOARCH" ./cmd/axiom-tempo-proxy

echo "copying binary and unit"
gcloud compute scp --zone "$ZONE" \
  "bin/axiom-tempo-proxy-linux-$GOARCH" deploy/axiom-tempo-proxy.service \
  "$INSTANCE:/tmp/"

echo "installing"
gcloud compute ssh "$INSTANCE" --zone "$ZONE" --command "
set -e
sudo useradd --system --no-create-home --shell /usr/sbin/nologin axiom-tempo-proxy 2>/dev/null || true
sudo install -m 0755 /tmp/axiom-tempo-proxy-linux-$GOARCH /usr/local/bin/axiom-tempo-proxy
sudo install -m 0644 /tmp/axiom-tempo-proxy.service /etc/systemd/system/axiom-tempo-proxy.service
sudo mkdir -p /etc/axiom-tempo-proxy
sudo tee /etc/axiom-tempo-proxy/env >/dev/null <<EOF
AXIOM_TOKEN=$AXIOM_TOKEN
AXIOM_DATASET=$DEFAULT_DATASET
PROXY_ALLOWED_DATASETS=$ALLOWED
PROXY_LISTEN_ADDR=$LISTEN
PROXY_LOG_LEVEL=info
EOF
sudo chmod 600 /etc/axiom-tempo-proxy/env
sudo chown root:axiom-tempo-proxy /etc/axiom-tempo-proxy/env
sudo chmod 640 /etc/axiom-tempo-proxy/env
rm -f /tmp/axiom-tempo-proxy-linux-$GOARCH /tmp/axiom-tempo-proxy.service
sudo systemctl daemon-reload
sudo systemctl enable --now axiom-tempo-proxy
sudo systemctl restart axiom-tempo-proxy
sleep 2
sudo systemctl --no-pager --lines=5 status axiom-tempo-proxy
curl -sS http://$LISTEN/api/echo && echo
"
echo "deployed to $INSTANCE ($LISTEN)"
