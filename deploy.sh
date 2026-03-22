#!/bin/bash
# Deploy source files to a remote Tunnel Manager instance
# Usage: ./deploy.sh <host:port> <session-token>
#
# Example:
#   ./deploy.sh 192.168.1.50:3000 abc123def456...
#
# The session token is the value of the "token" cookie after logging in.
# You can find it in your browser's dev tools (Application > Cookies).

HOST=$1
TOKEN=$2

if [ -z "$HOST" ] || [ -z "$TOKEN" ]; then
  echo "Usage: ./deploy.sh <host:port> <session-token>"
  echo ""
  echo "  host:port    Remote server address (e.g. 192.168.1.50:3000)"
  echo "  session-token  Value of the 'token' cookie from the browser"
  exit 1
fi

ARCHIVE="/tmp/tunnel-deploy-$$.tar.gz"

echo "Packing source files..."
tar czf "$ARCHIVE" \
  --exclude='node_modules' \
  --exclude='tunnels.json' \
  --exclude='profiles' \
  --exclude='.auth-tmp' \
  --exclude='deploy.sh' \
  server.js public/ oc-agent.js package.json install-vpn-servers.sh 2>/dev/null

if [ $? -ne 0 ]; then
  echo "Error: Failed to create archive"
  exit 1
fi

SIZE=$(du -h "$ARCHIVE" | cut -f1)
echo "Archive: $ARCHIVE ($SIZE)"
echo "Deploying to $HOST..."

RESPONSE=$(curl -s -w "\n%{http_code}" \
  -X POST "http://$HOST/api/deploy" \
  -H "Cookie: token=$TOKEN" \
  -F "archive=@$ARCHIVE")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

rm -f "$ARCHIVE"

if [ "$HTTP_CODE" = "200" ]; then
  echo "Success! $BODY"
  echo "Server will restart in ~2 seconds."
else
  echo "Deploy failed (HTTP $HTTP_CODE): $BODY"
  exit 1
fi
