"""Kafka consumer + realtime web graph.

Consumes the SNMP topic in a Socket.IO background task and pushes every
message to browsers as the "traffic" event. Flask-SocketIO serves the page on
0.0.0.0:WEB_PORT in the same process.
"""
import json
import sys
from collections import deque

from confluent_kafka import Consumer, KafkaError
from flask import Flask, render_template, request
from flask_socketio import SocketIO

import config

MAX_POINTS = 60

app = Flask(__name__)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")
history = deque(maxlen=MAX_POINTS)


def consume_loop():
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "snmp-worker",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.TOPIC])
    print(f"Consuming '{config.TOPIC}' from {config.KAFKA_BOOTSTRAP}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue
            try:
                data = json.loads(msg.value())
            except (TypeError, ValueError) as exc:
                print(f"Bad message skipped: {exc}", file=sys.stderr)
                continue
            print(f"{data.get('time')} {data.get('if_name')} "
                  f"IN {data.get('in_mbps')} Mbps OUT {data.get('out_mbps')} Mbps")
            history.append(data)
            socketio.emit("traffic", data)
    finally:
        consumer.close()


@app.route("/")
def index():
    return render_template("index.html", if_name=config.IF_NAME,
                           router_ip=config.ROUTER_IP, max_points=MAX_POINTS)


@app.route("/health")
def health():
    return {"status": "ok", "points": len(history)}


@socketio.on("connect")
def on_connect():
    socketio.emit("history", list(history), to=request.sid)


if __name__ == "__main__":
    socketio.start_background_task(consume_loop)
    print(f"Web graph on http://{config.SERVER_IP}:{config.WEB_PORT}")
    socketio.run(app, host="0.0.0.0", port=config.WEB_PORT,
                 allow_unsafe_werkzeug=True)
