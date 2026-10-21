"""SNMP collector (Kafka producer).

Polls ifHCInOctets / ifHCOutOctets of one router interface with net-snmp
`snmpget`, converts the counter deltas to bits per second and produces a JSON
message to the Kafka topic for every poll.
"""
import json
import signal
import subprocess
import sys
import time
from datetime import datetime

from confluent_kafka import KafkaException, Producer

import config

OID_IFNAME = "1.3.6.1.2.1.31.1.1.1.1"
OID_IN = "1.3.6.1.2.1.31.1.1.1.6"
OID_OUT = "1.3.6.1.2.1.31.1.1.1.10"
SNMP_TIMEOUT = "2"
SNMP_RETRIES = "1"


def snmp_cmd(tool, *args):
    cmd = [tool, "-v2c", "-c", config.COMMUNITY, "-t", SNMP_TIMEOUT,
           "-r", SNMP_RETRIES, *args]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "snmp error")
    return result.stdout


def find_if_index(if_name):
    """Walk ifName and return the ifIndex whose name equals if_name."""
    out = snmp_cmd("snmpwalk", "-On", config.ROUTER_IP, OID_IFNAME)
    for line in out.splitlines():
        # .1.3.6.1.2.1.31.1.1.1.1.2 = STRING: Fa1/1
        oid, _, value = line.partition(" = ")
        name = value.split(":", 1)[-1].strip().strip('"')
        if name == if_name:
            return oid.rsplit(".", 1)[-1]
    raise RuntimeError(f"interface {if_name} not found on {config.ROUTER_IP}")


def read_counters(if_index):
    out = snmp_cmd("snmpget", "-Oqv", config.ROUTER_IP,
                   f"{OID_IN}.{if_index}", f"{OID_OUT}.{if_index}")
    values = out.split()
    if len(values) != 2:
        raise RuntimeError(f"unexpected snmpget output: {out!r}")
    return int(values[0]), int(values[1])


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}", file=sys.stderr)


def on_sigterm(signum, frame):
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, on_sigterm)  # `kill` also flushes cleanly
    if_index = config.IF_INDEX
    while not if_index:
        try:
            if_index = find_if_index(config.IF_NAME)
            print(f"Found {config.IF_NAME} ifIndex = {if_index}")
        except Exception as exc:
            print(f"ifIndex lookup failed: {exc}; retry in {config.POLL_INTERVAL}s",
                  file=sys.stderr)
            time.sleep(config.POLL_INTERVAL)

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": "snmp-collector",
        "linger.ms": 0,
    })
    print(f"Polling {config.ROUTER_IP} {config.IF_NAME} (ifIndex {if_index}) "
          f"every {config.POLL_INTERVAL}s -> {config.KAFKA_BOOTSTRAP} topic "
          f"'{config.TOPIC}'. Ctrl+C to stop.")

    prev = None  # (timestamp, in_octets, out_octets)
    try:
        while True:
            started = time.time()
            try:
                in_oct, out_oct = read_counters(if_index)
                now = time.time()
            except Exception as exc:
                print(f"{datetime.now():%H:%M:%S} SNMP error: {exc}", file=sys.stderr)
                prev = None  # avoid a huge delta spanning the outage
            else:
                if prev is None:
                    print(f"{datetime.now():%H:%M:%S} {config.IF_NAME} first poll, "
                          f"baseline stored")
                else:
                    dt = now - prev[0]
                    d_in, d_out = in_oct - prev[1], out_oct - prev[2]
                    if dt <= 0 or d_in < 0 or d_out < 0:
                        print(f"{datetime.now():%H:%M:%S} counter reset or wrap, skipped")
                    else:
                        in_bps = d_in * 8 / dt
                        out_bps = d_out * 8 / dt
                        record = {
                            "router": "R1",
                            "ip": config.ROUTER_IP,
                            "if_index": int(if_index),
                            "if_name": config.IF_NAME,
                            "timestamp": now,
                            "time": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
                            "in_octets": in_oct,
                            "out_octets": out_oct,
                            "in_bps": round(in_bps, 2),
                            "out_bps": round(out_bps, 2),
                            "in_mbps": round(in_bps / 1e6, 3),
                            "out_mbps": round(out_bps / 1e6, 3),
                        }
                        try:
                            producer.produce(config.TOPIC, key=config.IF_NAME,
                                             value=json.dumps(record),
                                             callback=delivery_report)
                        except (BufferError, KafkaException) as exc:
                            print(f"Kafka produce error: {exc}", file=sys.stderr)
                        print(f"{datetime.fromtimestamp(now):%H:%M:%S} {config.IF_NAME} "
                              f"IN {record['in_mbps']:.2f} Mbps "
                              f"OUT {record['out_mbps']:.2f} Mbps")
                prev = (now, in_oct, out_oct)
            producer.poll(0)
            time.sleep(max(0.0, config.POLL_INTERVAL - (time.time() - started)))
    except KeyboardInterrupt:
        print("\nStopping, flushing Kafka producer...")
    finally:
        left = producer.flush(10)
        if left:
            print(f"{left} message(s) not delivered", file=sys.stderr)


if __name__ == "__main__":
    main()
