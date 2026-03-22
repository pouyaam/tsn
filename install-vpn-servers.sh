#!/bin/bash

# ─── VPN Servers Install Script ─────────────────────────────────────
# Installs: OpenVPN + easy-rsa, StrongSwan + xl2tpd (L2TP/IPsec),
#           redsocks, dns2socks, and configures everything.
#
# Usage:
#   sudo bash install-vpn-servers.sh              # fresh install
#   sudo bash install-vpn-servers.sh --reinstall   # force reinstall everything
#   sudo bash install-vpn-servers.sh --psk MyKey   # use specific L2TP PSK
#   sudo bash install-vpn-servers.sh --reinstall --psk MyKey
# ─────────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[x]${NC} $1"; exit 1; }

# ─── Parse arguments ─────────────────────────────────────────────────
REINSTALL=false
USER_PSK=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reinstall|-r)
      REINSTALL=true
      shift
      ;;
    --psk)
      USER_PSK="$2"
      shift 2
      ;;
    --psk=*)
      USER_PSK="${1#*=}"
      shift
      ;;
    *)
      warn "Unknown option: $1"
      shift
      ;;
  esac
done

# ─── Pre-checks ─────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && err "This script must be run as root (sudo)"
[[ ! -f /etc/debian_version ]] && err "This script is designed for Debian/Ubuntu"

PANEL_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULT_FILE="${PANEL_DIR}/vpn-install-result.json"
EASYRSA_DIR="/etc/openvpn/easy-rsa"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   VPN Servers Installer                      ║"
echo "║   OpenVPN + L2TP/IPsec + Redsocks            ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

if $REINSTALL; then
  warn "REINSTALL mode: will stop services and rebuild everything"
  echo ""
fi

# ─── 0. Stop all services first (critical for reinstall) ────────────
log "Stopping VPN services..."
systemctl stop openvpn@server 2>/dev/null || true
systemctl stop openvpn 2>/dev/null || true
systemctl stop strongswan-starter 2>/dev/null || true
systemctl stop strongswan 2>/dev/null || true
systemctl stop xl2tpd 2>/dev/null || true
systemctl stop redsocks 2>/dev/null || true
# Kill any leftover processes
killall -q charon 2>/dev/null || true
killall -q xl2tpd 2>/dev/null || true
killall -q openvpn 2>/dev/null || true
killall -q redsocks 2>/dev/null || true
killall -q dns2socks 2>/dev/null || true
sleep 1
log "Services stopped."

# ─── 1. Install packages ────────────────────────────────────────────
log "Updating package lists..."
apt-get update -qq || warn "apt-get update had warnings (continuing)"

log "Installing packages..."
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  openvpn easy-rsa \
  strongswan xl2tpd \
  ppp \
  redsocks \
  openconnect \
  sshpass \
  iptables iptables-persistent \
  build-essential gcc make \
  > /dev/null 2>&1 || err "Failed to install packages"

log "Packages installed."

# ─── 2. Build dns2socks ─────────────────────────────────────────────
if $REINSTALL || ! command -v dns2socks &> /dev/null; then
  log "Building dns2socks from source..."
  TMPBUILD=$(mktemp -d)

  cat > "$TMPBUILD/dns2socks.c" << 'DNSCODE'
/*
 * dns2socks - DNS to SOCKS5 proxy
 * Minimal implementation: listens on UDP, forwards DNS queries
 * as TCP through a SOCKS5 proxy.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/select.h>

#define BUF_SIZE 4096
#define DNS_PORT 53

static int socks5_connect(const char *socks_host, int socks_port,
                          const char *dest_host, int dest_port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(socks_port);
    inet_pton(AF_INET, socks_host, &sa.sin_addr);

    struct timeval tv = {10, 0};
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }

    /* SOCKS5 handshake: no auth */
    unsigned char greet[] = {0x05, 0x01, 0x00};
    if (send(fd, greet, 3, 0) != 3) { close(fd); return -1; }

    unsigned char resp[2];
    if (recv(fd, resp, 2, 0) != 2 || resp[1] != 0x00) { close(fd); return -1; }

    /* SOCKS5 connect to dest (IPv4) */
    unsigned char req[10];
    req[0] = 0x05; req[1] = 0x01; req[2] = 0x00; req[3] = 0x01;
    inet_pton(AF_INET, dest_host, req + 4);
    req[8] = (dest_port >> 8) & 0xff;
    req[9] = dest_port & 0xff;
    if (send(fd, req, 10, 0) != 10) { close(fd); return -1; }

    unsigned char cresp[10];
    if (recv(fd, cresp, 10, 0) < 4 || cresp[1] != 0x00) { close(fd); return -1; }

    return fd;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: dns2socks <socks_ip:port> <dns_ip> <listen_ip:port>\n");
        fprintf(stderr, "  e.g. dns2socks 127.0.0.1:1080 8.8.8.8 127.0.0.1:5353\n");
        return 1;
    }

    char socks_host[64]; int socks_port;
    if (sscanf(argv[1], "%63[^:]:%d", socks_host, &socks_port) != 2) {
        fprintf(stderr, "Invalid SOCKS address: %s\n", argv[1]);
        return 1;
    }

    char *dns_host = argv[2];

    char listen_host[64] = "127.0.0.1"; int listen_port = 5353;
    if (sscanf(argv[3], "%63[^:]:%d", listen_host, &listen_port) != 2) {
        listen_port = atoi(argv[3]);
        strcpy(listen_host, "127.0.0.1");
    }

    /* Create UDP listener */
    int udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(udp_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_host, &bind_addr.sin_addr);

    if (bind(udp_fd, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
        perror("bind");
        return 1;
    }

    fprintf(stderr, "[dns2socks] Listening on %s:%d, SOCKS %s:%d, DNS %s:53\n",
            listen_host, listen_port, socks_host, socks_port, dns_host);

    unsigned char buf[BUF_SIZE];
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t clen = sizeof(client_addr);
        ssize_t n = recvfrom(udp_fd, buf, BUF_SIZE, 0,
                             (struct sockaddr *)&client_addr, &clen);
        if (n <= 0) continue;

        /* Connect to DNS server via SOCKS5 (TCP) */
        int tcp_fd = socks5_connect(socks_host, socks_port, dns_host, DNS_PORT);
        if (tcp_fd < 0) continue;

        /* DNS over TCP: prepend 2-byte length */
        unsigned char lenbuf[2];
        lenbuf[0] = (n >> 8) & 0xff;
        lenbuf[1] = n & 0xff;
        if (send(tcp_fd, lenbuf, 2, 0) != 2 ||
            send(tcp_fd, buf, n, 0) != n) {
            close(tcp_fd);
            continue;
        }

        /* Read response length */
        if (recv(tcp_fd, lenbuf, 2, 0) != 2) { close(tcp_fd); continue; }
        int rlen = (lenbuf[0] << 8) | lenbuf[1];
        if (rlen > BUF_SIZE) { close(tcp_fd); continue; }

        /* Read response */
        int total = 0;
        while (total < rlen) {
            ssize_t r = recv(tcp_fd, buf + total, rlen - total, 0);
            if (r <= 0) break;
            total += r;
        }
        close(tcp_fd);

        if (total == rlen) {
            sendto(udp_fd, buf, rlen, 0,
                   (struct sockaddr *)&client_addr, clen);
        }
    }

    return 0;
}
DNSCODE

  gcc -O2 -o "$TMPBUILD/dns2socks" "$TMPBUILD/dns2socks.c" || err "Failed to compile dns2socks"
  cp "$TMPBUILD/dns2socks" /usr/local/bin/dns2socks
  chmod +x /usr/local/bin/dns2socks
  rm -rf "$TMPBUILD"
  log "dns2socks built and installed to /usr/local/bin/dns2socks"
else
  log "dns2socks already installed, skipping. (use --reinstall to force)"
fi

# ─── 3. Enable IP forwarding ────────────────────────────────────────
log "Enabling IP forwarding..."
cat > /etc/sysctl.d/99-vpn-forward.conf << 'EOF'
net.ipv4.ip_forward = 1
net.ipv4.conf.all.route_localnet = 1
net.ipv4.conf.all.accept_redirects = 0
net.ipv4.conf.all.send_redirects = 0
net.ipv4.conf.default.accept_redirects = 0
net.ipv4.conf.default.send_redirects = 0
EOF
sysctl -p /etc/sysctl.d/99-vpn-forward.conf > /dev/null 2>&1

# ─── 3b. Load L2TP kernel modules ──────────────────────────────────
log "Loading L2TP kernel modules..."
modprobe l2tp_ppp 2>/dev/null || warn "Could not load l2tp_ppp module (may be built-in)"
modprobe pppol2tp 2>/dev/null || warn "Could not load pppol2tp module (may be built-in)"

# Persist modules across reboots
for mod in l2tp_ppp pppol2tp; do
  grep -qx "$mod" /etc/modules 2>/dev/null || echo "$mod" >> /etc/modules
done
log "L2TP kernel modules loaded."

# ─── 4. Initialize OpenVPN PKI ──────────────────────────────────────
if $REINSTALL || [[ ! -d "${EASYRSA_DIR}/pki" ]]; then
  if $REINSTALL && [[ -d "${EASYRSA_DIR}" ]]; then
    warn "Reinstall: wiping entire EasyRSA directory"
    rm -rf "${EASYRSA_DIR}"
  fi

  log "Initializing OpenVPN PKI..."
  make-cadir "${EASYRSA_DIR}" 2>/dev/null || {
    # make-cadir fails if dir partially exists — manually set up
    mkdir -p "${EASYRSA_DIR}"
    # Copy easyrsa script and openssl config from the installed package
    if [[ -f /usr/share/easy-rsa/easyrsa ]]; then
      cp -r /usr/share/easy-rsa/* "${EASYRSA_DIR}/"
    fi
  }

  # Set vars for non-interactive operation
  cat > "${EASYRSA_DIR}/vars" << 'EOF'
set_var EASYRSA_BATCH     "yes"
set_var EASYRSA_KEY_SIZE  2048
set_var EASYRSA_ALGO      rsa
set_var EASYRSA_CA_EXPIRE 3650
set_var EASYRSA_CERT_EXPIRE 3650
set_var EASYRSA_CRL_DAYS  3650
EOF

  cd "${EASYRSA_DIR}"
  ./easyrsa init-pki || err "Failed to init PKI"
  EASYRSA_REQ_CN="VPN-Panel-CA" ./easyrsa build-ca nopass || err "Failed to build CA"
  ./easyrsa gen-dh || err "Failed to generate DH params"
  ./easyrsa build-server-full server nopass || err "Failed to build server cert"
  ./easyrsa gen-crl || err "Failed to generate CRL"

  # Generate tls-auth key (try new syntax first, fall back to old)
  openvpn --genkey tls-auth "${EASYRSA_DIR}/ta.key" 2>/dev/null || \
    openvpn --genkey secret "${EASYRSA_DIR}/ta.key" || err "Failed to generate ta.key"

  cd "${PANEL_DIR}"
  log "PKI initialized: CA, server cert, DH params, ta.key, CRL"
else
  warn "PKI already exists at ${EASYRSA_DIR}/pki, skipping. (use --reinstall to force)"
fi

# ─── 5. Write OpenVPN server config ─────────────────────────────────
log "Writing OpenVPN server config..."
mkdir -p /etc/openvpn
cat > /etc/openvpn/server.conf << EOF
port 1194
proto udp
dev tun
ca ${EASYRSA_DIR}/pki/ca.crt
cert ${EASYRSA_DIR}/pki/issued/server.crt
key ${EASYRSA_DIR}/pki/private/server.key
dh ${EASYRSA_DIR}/pki/dh.pem
tls-auth ${EASYRSA_DIR}/ta.key 0
crl-verify ${EASYRSA_DIR}/pki/crl.pem
topology subnet
server 10.8.0.0 255.255.255.0
push "redirect-gateway def1 bypass-dhcp"
push "dhcp-option DNS 8.8.8.8"
push "dhcp-option DNS 8.8.4.4"
keepalive 10 120
cipher AES-256-GCM
data-ciphers AES-256-GCM:AES-128-GCM:CHACHA20-POLY1305
persist-key
persist-tun
tun-mtu 1280
mssfix 1200
status /var/log/openvpn-status.log 10
log-append /var/log/openvpn.log
verb 3
duplicate-cn
max-clients 50
EOF

# ─── 6. Write StrongSwan config ─────────────────────────────────────
log "Writing StrongSwan/IPsec config..."
cat > /etc/ipsec.conf << 'EOF'
config setup
    charondebug="ike 1, knl 1, cfg 0"
    uniqueids=no

conn L2TP-PSK
    keyexchange=ikev1
    authby=secret
    auto=add
    keyingtries=3
    rekey=no
    ikelifetime=8h
    keylife=1h
    type=transport
    left=%defaultroute
    leftprotoport=17/1701
    right=%any
    rightprotoport=17/%any
    forceencaps=yes
    dpddelay=30
    dpdtimeout=120
    dpdaction=clear
    ike=aes256-sha256-modp2048,aes128-sha1-modp2048,3des-sha1-modp1024!
    esp=aes256-sha256,aes128-sha1,3des-sha1!
EOF

# ─── 7. Determine L2TP Pre-Shared Key ───────────────────────────────
if [[ -n "$USER_PSK" ]]; then
  PSK="$USER_PSK"
  log "Using provided PSK"
elif ! $REINSTALL && [[ -f /etc/ipsec.secrets ]]; then
  # Preserve existing PSK on normal install
  EXISTING_PSK=$(grep -oP 'PSK\s+"\K[^"]+' /etc/ipsec.secrets 2>/dev/null || true)
  if [[ -n "$EXISTING_PSK" ]]; then
    PSK="$EXISTING_PSK"
    log "Preserving existing PSK"
  else
    PSK=$(openssl rand -hex 16)
    log "Generated new random PSK"
  fi
else
  PSK=$(openssl rand -hex 16)
  log "Generated new random PSK"
fi

echo ": PSK \"${PSK}\"" > /etc/ipsec.secrets
chmod 600 /etc/ipsec.secrets

# ─── 8. Write xl2tpd config ─────────────────────────────────────────
log "Writing xl2tpd config..."
mkdir -p /etc/xl2tpd

cat > /etc/xl2tpd/xl2tpd.conf << 'EOF'
[global]
port = 1701

[lns default]
ip range = 10.9.0.10-10.9.0.250
local ip = 10.9.0.1
require chap = yes
refuse pap = yes
require authentication = yes
name = l2tpd
pppoptfile = /etc/ppp/options.xl2tpd
length bit = yes
EOF

# ─── 9. Write PPP options for L2TP ──────────────────────────────────
log "Writing PPP options..."
mkdir -p /etc/ppp
cat > /etc/ppp/options.xl2tpd << 'EOF'
ipcp-accept-local
ipcp-accept-remote
require-mschap-v2
refuse-eap
refuse-pap
refuse-chap
refuse-mschap
ms-dns 8.8.8.8
ms-dns 8.8.4.4
noccp
nodefaultroute
auth
mtu 1280
mru 1280
proxyarp
lcp-echo-failure 4
lcp-echo-interval 30
connect-delay 1000
logfile /var/log/pppd.log
EOF

# Create chap-secrets if not exists (don't overwrite — panel manages it)
if [[ ! -f /etc/ppp/chap-secrets ]]; then
  touch /etc/ppp/chap-secrets
fi
chmod 600 /etc/ppp/chap-secrets

# ─── 10. Write initial redsocks config ───────────────────────────────
log "Writing redsocks config..."
cat > /etc/redsocks.conf << 'EOF'
base {
    log_debug = off;
    log_info = on;
    daemon = off;
    redirector = iptables;
}

redsocks {
    local_ip = 0.0.0.0;
    local_port = 12345;
    ip = 127.0.0.1;
    port = 1080;
    type = socks5;
}
EOF

# ─── 11. Disable auto-start (panel manages services) ────────────────
log "Disabling service auto-start (panel will manage them)..."
systemctl disable openvpn@server 2>/dev/null || true
systemctl disable strongswan-starter 2>/dev/null || true
systemctl disable strongswan 2>/dev/null || true
systemctl disable xl2tpd 2>/dev/null || true
systemctl disable redsocks 2>/dev/null || true

# ─── 12. Open firewall ports ────────────────────────────────────────
log "Adding iptables rules for VPN ports..."
# Use -C to check if rule exists, add only if missing
iptables -C INPUT -p udp --dport 500  -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 500  -j ACCEPT
iptables -C INPUT -p udp --dport 4500 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 4500 -j ACCEPT
iptables -C INPUT -p udp --dport 1701 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 1701 -j ACCEPT
iptables -C INPUT -p udp --dport 1194 -j ACCEPT 2>/dev/null || iptables -I INPUT -p udp --dport 1194 -j ACCEPT
iptables -C INPUT -p esp -j ACCEPT 2>/dev/null || iptables -I INPUT -p esp -j ACCEPT

# Also allow established/related for return traffic
iptables -C INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || \
  iptables -I INPUT -m state --state RELATED,ESTABLISHED -j ACCEPT

# Save iptables rules so they survive reboot
mkdir -p /etc/iptables
iptables-save > /etc/iptables/rules.v4 2>/dev/null || true

if command -v ufw &> /dev/null; then
  log "Opening UFW firewall ports..."
  ufw allow 1194/udp comment "OpenVPN" 2>/dev/null || true
  ufw allow 500/udp  comment "IKE"     2>/dev/null || true
  ufw allow 4500/udp comment "NAT-T"   2>/dev/null || true
  ufw allow 1701/udp comment "L2TP"    2>/dev/null || true
fi

# ─── 13. Verify installation ────────────────────────────────────────
log "Verifying installation..."
ERRORS=""

if [[ ! -f "${EASYRSA_DIR}/pki/ca.crt" ]]; then ERRORS+="  - CA certificate missing\n"; fi
if [[ ! -f "${EASYRSA_DIR}/pki/issued/server.crt" ]]; then ERRORS+="  - Server certificate missing\n"; fi
if [[ ! -f "${EASYRSA_DIR}/pki/dh.pem" ]]; then ERRORS+="  - DH params missing\n"; fi
if [[ ! -f "${EASYRSA_DIR}/ta.key" ]]; then ERRORS+="  - ta.key missing\n"; fi
if [[ ! -f /etc/ipsec.conf ]]; then ERRORS+="  - ipsec.conf missing\n"; fi
if [[ ! -f /etc/ipsec.secrets ]]; then ERRORS+="  - ipsec.secrets missing\n"; fi
if [[ ! -f /etc/xl2tpd/xl2tpd.conf ]]; then ERRORS+="  - xl2tpd.conf missing\n"; fi
if ! command -v dns2socks &> /dev/null; then ERRORS+="  - dns2socks binary missing\n"; fi
if ! command -v ipsec &> /dev/null; then ERRORS+="  - ipsec (strongswan) not found\n"; fi
if ! command -v xl2tpd &> /dev/null; then ERRORS+="  - xl2tpd not found\n"; fi
if ! command -v openvpn &> /dev/null; then ERRORS+="  - openvpn not found\n"; fi
if ! command -v redsocks &> /dev/null; then ERRORS+="  - redsocks not found\n"; fi

if [[ -n "$ERRORS" ]]; then
  warn "Some components have issues:"
  echo -e "$ERRORS"
else
  log "All components verified OK"
fi

# ─── 14. Quick L2TP connectivity test ───────────────────────────────
log "Testing L2TP stack (start → verify → stop)..."
modprobe l2tp_ppp 2>/dev/null || true
modprobe pppol2tp 2>/dev/null || true
ipsec restart 2>/dev/null
systemctl restart xl2tpd 2>/dev/null
sleep 2

# Check if charon is actually listening
if ss -ulnp | grep -q ':500 '; then
  log "IPsec (charon) listening on UDP 500/4500"
else
  warn "IPsec NOT listening on UDP 500 — check: journalctl -u strongswan"
fi

if ss -ulnp | grep -q ':1701 '; then
  log "xl2tpd listening on UDP 1701"
else
  warn "xl2tpd NOT listening on UDP 1701 — check: journalctl -u xl2tpd"
fi

# Stop again — panel will manage
ipsec stop 2>/dev/null || true
systemctl stop xl2tpd 2>/dev/null || true

# ─── 15. Save result for panel ──────────────────────────────────────
cat > "${RESULT_FILE}" << RESULT
{
  "installed": true,
  "timestamp": "$(date -Iseconds)",
  "reinstall": $REINSTALL,
  "psk": "${PSK}",
  "pkiInitialized": true,
  "pkiDir": "${EASYRSA_DIR}",
  "openvpn": {
    "configFile": "/etc/openvpn/server.conf",
    "statusLog": "/var/log/openvpn-status.log",
    "log": "/var/log/openvpn.log",
    "subnet": "10.8.0.0/24",
    "port": 1194,
    "proto": "udp"
  },
  "l2tp": {
    "subnet": "10.9.0.0/24",
    "ipRange": "10.9.0.10-10.9.0.250",
    "localIp": "10.9.0.1",
    "chapSecrets": "/etc/ppp/chap-secrets"
  },
  "redsocks": {
    "configFile": "/etc/redsocks.conf",
    "localPort": 12345
  },
  "dns2socks": {
    "binary": "/usr/local/bin/dns2socks",
    "listenPort": 5353
  }
}
RESULT

# ─── Done ────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Installation Complete!                     ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
log "OpenVPN server config:  /etc/openvpn/server.conf"
log "OpenVPN PKI:            ${EASYRSA_DIR}/pki/"
log "StrongSwan config:      /etc/ipsec.conf"
log "xl2tpd config:          /etc/xl2tpd/xl2tpd.conf"
log "PPP chap-secrets:       /etc/ppp/chap-secrets"
log "Redsocks config:        /etc/redsocks.conf"
log "dns2socks binary:       /usr/local/bin/dns2socks"
echo ""
echo -e "${GREEN}L2TP Pre-Shared Key:${NC} ${PSK}"
echo ""
if $REINSTALL; then
  log "REINSTALL complete. All components rebuilt."
fi
log "All services are STOPPED. The panel will start them on demand."
log "Result saved to: ${RESULT_FILE}"
