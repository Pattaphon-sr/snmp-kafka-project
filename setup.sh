#!/usr/bin/env bash
# Idempotent setup for snmp-kafka-project on Ubuntu.
# Checks first, installs/creates only what is missing, never deletes anything.
# Run from the project folder:  bash setup.sh
set -u
cd "$(dirname "$0")"

ROUTER_IP="${ROUTER_IP:-192.168.80.135}"
COMMUNITY="${COMMUNITY:-public}"
IF_NAME="${IF_NAME:-Fa1/1}"
TOPIC="${TOPIC:-snmp}"

step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m[OK]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!!]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; exit 1; }

step "1. Server IP"
SERVER_IP="${SERVER_IP:-$(hostname -I | awk '{print $1}')}"
echo "hostname -I: $(hostname -I)"
ok "SERVER_IP=$SERVER_IP (expected 192.168.80.131)"

step "2. net-snmp + ifIndex of $IF_NAME"
if ! command -v snmpwalk >/dev/null; then
  sudo apt-get update && sudo apt-get install -y snmp || fail "apt install snmp failed"
fi
ok "$(snmpwalk -V 2>&1)"
WALK=$(snmpwalk -v2c -c "$COMMUNITY" -t 2 -r 1 -On "$ROUTER_IP" 1.3.6.1.2.1.31.1.1.1.1 2>&1) \
  || fail "Router $ROUTER_IP did not answer SNMP: $WALK"
echo "$WALK"
IF_INDEX=$(echo "$WALK" | awk -v n="$IF_NAME" -F' = STRING: ' '{gsub(/"/,"",$2)} $2==n {sub(/.*\./,"",$1); print $1}')
[ -n "$IF_INDEX" ] || fail "$IF_NAME not found in ifName table"
ok "IF_INDEX=$IF_INDEX"

step "3. Docker + compose plugin"
if ! command -v docker >/dev/null; then
  sudo apt-get update && sudo apt-get install -y docker.io || fail "install docker.io failed"
fi
if ! docker compose version >/dev/null 2>&1 && ! sudo docker compose version >/dev/null 2>&1; then
  sudo apt-get install -y docker-compose-v2 || fail "install docker-compose-v2 failed"
fi
if docker info >/dev/null 2>&1; then DOCKER="docker"; else DOCKER="sudo docker"; fi
ok "$($DOCKER --version); $($DOCKER compose version)"
[ "$DOCKER" = "docker" ] || warn "user $USER is not in group docker (using sudo). Optional: sudo usermod -aG docker $USER, then log in again"

step "4. Kafka + kafka-ui"
$DOCKER ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
# A broker = a known Kafka image, or whatever publishes host port 9092
KAFKA_C=$($DOCKER ps -a --format '{{.Names}}|{{.Image}}|{{.Ports}}' | awk -F'|' \
  '$2 ~ /^(apache\/kafka|bitnami\/kafka|confluentinc\/cp-(kafka|server))(:|$)/ || $3 ~ /:9092->/ {print $1; exit}')
UI_C=$($DOCKER ps -a --format '{{.Names}} {{.Image}}' | awk '$2 ~ /kafka-ui/ {print $1; exit}')
echo "Listening ports 9092/8080:"; ss -ltn '( sport = :9092 or sport = :8080 )'
if [ -n "$KAFKA_C" ]; then
  ok "Reusing existing Kafka container: $KAFKA_C"
  [ "$($DOCKER inspect -f '{{.State.Running}}' "$KAFKA_C")" = true ] || $DOCKER start "$KAFKA_C"
  [ -n "$UI_C" ] && { [ "$($DOCKER inspect -f '{{.State.Running}}' "$UI_C")" = true ] || $DOCKER start "$UI_C"; }
  [ -z "$UI_C" ] && warn "No kafka-ui container found; add one manually if you want the UI"
elif ss -ltn '( sport = :9092 )' | grep -q 9092; then
  fail "Port 9092 is used by a non-Docker process; not touching it. Check: sudo ss -ltnp 'sport = :9092'"
else
  SERVER_IP="$SERVER_IP" $DOCKER compose -f docker-compose.standalone.yml up -d || fail "docker compose up failed"
  KAFKA_C=kafka; UI_C=kafka-ui
fi
# Inside the container use the INTERNAL listener (the advertised SERVER_IP:9092 may not hairpin)
BS=$($DOCKER inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$KAFKA_C" \
  | sed -n 's/^KAFKA_ADVERTISED_LISTENERS=.*INTERNAL:\/\/\([^,]*\).*/\1/p')
BS=${BS:-localhost:9092}
echo -n "Waiting for Kafka "
KT=""
for i in $(seq 1 40); do
  for p in /opt/kafka/bin/kafka-topics.sh kafka-topics kafka-topics.sh /opt/bitnami/kafka/bin/kafka-topics.sh; do
    if $DOCKER exec "$KAFKA_C" sh -c "command -v $p || test -x $p" >/dev/null 2>&1; then KT=$p; break; fi
  done
  [ -n "$KT" ] && $DOCKER exec "$KAFKA_C" $KT --bootstrap-server $BS --list >/dev/null 2>&1 && break
  echo -n "."; sleep 3
done; echo
[ -n "$KT" ] || fail "kafka-topics tool not found in $KAFKA_C"
ok "Kafka ready ($KAFKA_C, in-container bootstrap $BS)"
UI_PORT=$([ -n "$UI_C" ] && $DOCKER port "$UI_C" 8080/tcp 2>/dev/null | head -1 | sed 's/.*://')
UI_PORT=${UI_PORT:-8080}

step "5. Topic $TOPIC"
$DOCKER exec "$KAFKA_C" $KT --bootstrap-server $BS --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1
$DOCKER exec "$KAFKA_C" $KT --bootstrap-server $BS --list
$DOCKER exec "$KAFKA_C" $KT --bootstrap-server $BS --describe --topic "$TOPIC"

step "6. Python venv"
if ! python3 -m venv --help >/dev/null 2>&1 || ! python3 -c 'import ensurepip' 2>/dev/null; then
  sudo apt-get install -y python3-venv
fi
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q --upgrade pip && .venv/bin/pip install -q -r requirements.txt || fail "pip install failed"
ok "$(.venv/bin/pip list 2>/dev/null | grep -Ei 'confluent|flask|socketio|websocket' | tr -s ' ' | paste -sd ', ')"

# Persist discovered values for the Python apps (env vars override config.py defaults)
cat > lab.env <<EOF
export ROUTER_IP=$ROUTER_IP
export COMMUNITY=$COMMUNITY
export IF_NAME=$IF_NAME
export IF_INDEX=$IF_INDEX
export SERVER_IP=$SERVER_IP
export KAFKA_BOOTSTRAP=$SERVER_IP:9092
export TOPIC=$TOPIC
EOF
ok "wrote lab.env"

step "10. 30-second end-to-end test"
. ./lab.env
.venv/bin/python -u worker.py > worker.log 2>&1 & WPID=$!
sleep 4
.venv/bin/python -u collector.py > collector.log 2>&1 & CPID=$!
sleep 30
HTTP=$(curl -s -o /dev/null -w '%{http_code}' "http://$SERVER_IP:5000/")
kill -TERM $CPID 2>/dev/null; sleep 3; kill $WPID 2>/dev/null; wait 2>/dev/null
echo "--- collector.log"; cat collector.log
echo "--- worker.log (last 10)"; tail -n 10 worker.log
COUNT=$($DOCKER exec "$KAFKA_C" sh -c "timeout 15 \$(dirname $KT)/kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --from-beginning --timeout-ms 8000 2>/dev/null || timeout 15 kafka-console-consumer --bootstrap-server $BS --topic $TOPIC --from-beginning --timeout-ms 8000 2>/dev/null" | grep -c in_mbps)
[ "$COUNT" -gt 0 ] && ok "$COUNT message(s) in topic $TOPIC" || warn "no messages found in topic $TOPIC"
[ "$HTTP" = 200 ] && ok "http://$SERVER_IP:5000 -> HTTP 200" || warn "http://$SERVER_IP:5000 -> HTTP $HTTP"

if ! command -v ufw >/dev/null; then
  ok "ufw not installed"
elif [ -t 0 ]; then UFW=$(sudo ufw status); else UFW=$(sudo -n ufw status 2>&1); fi
case "${UFW:-}" in
  *"Status: active"*) warn "ufw is active. Open the ports with: sudo ufw allow 5000/tcp; sudo ufw allow ${UI_PORT}/tcp; sudo ufw allow 9092/tcp" ;;
  *"Status: inactive"*) ok "ufw is not active" ;;
  "") ;;
  *) warn "could not read ufw status (needs sudo): run 'sudo ufw status'" ;;
esac

step "Summary"
echo "IF_INDEX         : $IF_INDEX ($IF_NAME)"
echo "Kafka bootstrap  : $SERVER_IP:9092"
echo "kafka-ui         : http://$SERVER_IP:$UI_PORT"
echo "Graph            : http://$SERVER_IP:5000"
