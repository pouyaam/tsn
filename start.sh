#!/bin/bash
cat > /etc/resolv.conf << 'EOF'
nameserver 78.157.42.100
nameserver 2.144.6.75
nameserver 178.22.122.100
nameserver 185.51.200.2
EOF

cd "$(dirname "$0")"
OLD_PID=$(lsof -ti :3000 2>/dev/null)
if [ -n "$OLD_PID" ]; then
  kill $OLD_PID 2>/dev/null
  sleep 1
  echo "Killed old process (PID: $OLD_PID)"
fi
nohup npm run dev > /dev/null 2>&1 &
echo "Started (PID: $!)"
