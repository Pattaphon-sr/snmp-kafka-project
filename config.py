"""Lab settings. Every value can be overridden with an environment variable."""
import os

ROUTER_IP = os.getenv("ROUTER_IP", "192.168.80.135")
COMMUNITY = os.getenv("COMMUNITY", "public")
IF_NAME = os.getenv("IF_NAME", "Fa1/1")
# Leave empty to auto-discover the ifIndex of IF_NAME via SNMP (ifName walk).
IF_INDEX = os.getenv("IF_INDEX", "3")
SERVER_IP = os.getenv("SERVER_IP", "192.168.80.131")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", f"{SERVER_IP}:9092")
TOPIC = os.getenv("TOPIC", "snmp")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
WEB_PORT = int(os.getenv("WEB_PORT", "5000"))
