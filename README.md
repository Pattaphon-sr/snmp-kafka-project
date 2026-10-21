# SNMP Kafka Realtime Traffic Graph

โปรเจกต์ Lab สำหรับดึงค่า traffic ของ interface `FastEthernet1/1` บน Cisco 7200 (R1 ใน GNS3) ผ่าน SNMP v2c แล้วส่งเข้า Apache Kafka topic `snmp` จากนั้น worker จะอ่านข้อมูลจาก Kafka และส่งต่อไปยังหน้าเว็บแบบ realtime ด้วย Socket.IO เพื่อแสดงเป็นกราฟ IN/OUT Mbps

## ภาพรวมระบบ

```text
Cisco 7200 R1 (192.168.80.135, Fa1/1)
    | SNMP v2c :161 (snmpget ifHCInOctets / ifHCOutOctets)
    v
collector.py  (Kafka producer, poll ทุก 5 วินาที)
    | Kafka PLAINTEXT 192.168.80.131:9092
    v
Kafka topic "snmp"  (apache/kafka 4.3.1 KRaft, container เดิมจาก ~/kafka_lab)
    |
    v
worker.py  (Kafka consumer + Flask-SocketIO :5000)
    | Socket.IO event "traffic"
    v
Web Browser (Chart.js realtime graph)
```

## ความสามารถ

- ค้นหา `ifIndex` ของ `Fa1/1` อัตโนมัติจากตาราง `ifName` (หรือกำหนดเองด้วย `IF_INDEX`)
- อ่าน 64-bit counter `ifHCInOctets` / `ifHCOutOctets` ด้วย `snmpget`
- คำนวณ `bps = delta_octets * 8 / delta_seconds` โดยข้าม poll แรกและค่า delta ติดลบ (counter reset)
- ส่งข้อมูล JSON เข้า Kafka topic `snmp`
- ทำงานต่อได้เมื่อ SNMP timeout หรือ Kafka error และ flush ข้อมูลเมื่อกด Ctrl+C
- worker เก็บข้อมูลล่าสุด 60 จุด และส่งให้ browser ที่เพิ่งเชื่อมต่อทันที
- หน้าเว็บแสดงค่าปัจจุบันตัวใหญ่ และกราฟเส้น IN/OUT สูงสุด 60 จุด
- ใช้ kafka-ui เดิมสำหรับดู topic และ message ที่ port `8082` (port `8080` เป็นของ `myapp-app-1`)
- มี `setup.sh` สำหรับตรวจสอบ ติดตั้ง และทดสอบทั้งระบบ โดยไม่ลบ container, volume หรือไฟล์เดิม

## โครงสร้างโปรเจกต์

```text
.
├── config.py              # ค่า Lab ทั้งหมด (override ได้ด้วย environment variable)
├── collector.py           # SNMP poller + Kafka producer
├── worker.py              # Kafka consumer + Flask-SocketIO web server
├── templates/
│   └── index.html         # หน้าเว็บกราฟ realtime (Chart.js + Socket.IO client)
├── requirements.txt       # Python dependencies
├── docker-compose.standalone.yml  # Kafka 3.8 + kafka-ui สำรอง (ใช้เมื่อ server ไม่มี Kafka เท่านั้น)
├── setup.sh               # สคริปต์ตรวจสอบ/ติดตั้ง/ทดสอบแบบ idempotent
├── lab.env                # สร้างโดย setup.sh (IF_INDEX, KAFKA_BOOTSTRAP ฯลฯ)
├── .gitignore
└── README.md
```

## ค่าที่ใช้ใน Lab

กำหนดไว้ใน `config.py` และ override ได้ด้วย environment variable (`setup.sh` จะเขียนค่าที่ตรวจพบลง `lab.env`)

```dotenv
ROUTER_IP=192.168.80.135
COMMUNITY=public
IF_NAME=Fa1/1
IF_INDEX=3           # ตั้งเป็นค่าว่าง = ค้นหาอัตโนมัติจาก ifName
SERVER_IP=192.168.80.131
KAFKA_BOOTSTRAP=192.168.80.131:9092
TOPIC=snmp
POLL_INTERVAL=5
WEB_PORT=5000
```

ตัวอย่างข้อความใน topic `snmp`:

```json
{"router": "R1", "ip": "192.168.80.135", "if_index": 3, "if_name": "Fa1/1",
 "timestamp": 1792560005.12, "time": "2026-10-21T12:00:05",
 "in_octets": 123456789, "out_octets": 2345678,
 "in_bps": 4980000.0, "out_bps": 120000.0, "in_mbps": 4.98, "out_mbps": 0.12}
```

## สิ่งที่ต้องมี

- Ubuntu Server (`192.168.80.131`) ที่ติดต่อ Router `192.168.80.135` UDP `161` ได้
- เปิด SNMP บน R1 แล้ว เช่น `snmp-server community public RO`
- Docker และ Docker Compose plugin (`setup.sh` ติดตั้งให้หากยังไม่มี)
- Kafka ที่รันอยู่แล้ว: container `kafka` (project `~/kafka_lab`) ภายนอกใช้ `192.168.80.131:9092` ภายใน container ใช้ `kafka:19092`
- Python 3 และ `python3-venv`
- พอร์ต TCP `5000` ว่าง และ `8082`, `9092` เข้าถึงได้จากเครื่องผู้ใช้

ตรวจสอบโปรแกรมและพอร์ต:

```bash
docker --version
docker compose version
snmpwalk -v2c -c public 192.168.80.135 1.3.6.1.2.1.31.1.1.1.1
ss -ltnp '( sport = :5000 or sport = :8082 or sport = :9092 )'
```

## วิธีติดตั้งและรัน

ติดตั้งและทดสอบทั้งหมดด้วยคำสั่งเดียว (ระบบจะถาม sudo password เมื่อต้องติดตั้งแพ็กเกจ):

```bash
cd ~/snmp-kafka-project
bash setup.sh
```

`setup.sh` จะตรวจหา Kafka และ kafka-ui ที่มีอยู่แล้วและใช้ของเดิม หากไม่พบ Kafka เลยจึงจะรัน `docker-compose.standalone.yml` ให้

หรือทำทีละขั้น:

```bash
cd ~/snmp-kafka-project
sudo apt install -y snmp python3-venv
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092   --create --if-not-exists --topic snmp --partitions 1 --replication-factor 1
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

รันระบบ (เปิด 2 terminal):

```bash
# Terminal 1 - worker + web graph
cd ~/snmp-kafka-project && source lab.env && .venv/bin/python worker.py

# Terminal 2 - SNMP collector
cd ~/snmp-kafka-project && source lab.env && .venv/bin/python collector.py
```

ตัวอย่าง output ของ collector:

```text
12:00:05 Fa1/1 IN 4.98 Mbps OUT 0.12 Mbps
```

สร้าง traffic ผ่าน Fa1/1 ด้วย iperf3 ประมาณ 5 Mbps:

```bash
iperf3 -s                               # ฝั่งปลายทาง
iperf3 -c <IPERF_SERVER_IP> -b 5M -t 300 # ฝั่งต้นทาง
```

เปิดหน้าเว็บที่:

```text
กราฟ realtime : http://192.168.80.131:5000
kafka-ui      : http://192.168.80.131:8082
```

## ตรวจสอบข้อมูลใน Kafka

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic snmp --from-beginning
curl http://127.0.0.1:5000/health
```

## คำสั่งบริหาร Kafka

Kafka และ kafka-ui เป็น container เดิมจาก compose project `~/kafka_lab` (ตั้ง `restart: unless-stopped` จึงเริ่มเองหลัง reboot)

```bash
cd ~/kafka_lab
docker compose ps
docker compose logs -f kafka
docker compose start      # เริ่ม Kafka และ kafka-ui
docker compose stop       # หยุดโดยไม่ลบ container/ข้อมูล
```

> การ stop Kafka จะกระทบโปรเจกต์ `kafka_cisco_lab` (cisco-backend1 และ worker) ที่ใช้ Kafka ตัวเดียวกันด้วย และไม่ควรใช้ `docker compose down -v` เพราะจะลบ volume และข้อมูลใน topic ทั้งหมด

## ขั้นตอนการทำงาน

1. `collector.py` ค้นหา ifIndex ของ `Fa1/1` จาก OID `1.3.6.1.2.1.31.1.1.1.1` (ifName)
2. ทุก 5 วินาทีเรียก `snmpget -v2c -c public -Oqv` อ่าน `ifHCInOctets` (`.6.IF_INDEX`) และ `ifHCOutOctets` (`.10.IF_INDEX`)
3. คำนวณ IN/OUT bps จากผลต่าง counter และเวลา แล้ว produce JSON เข้า topic `snmp`
4. `worker.py` consume topic `snmp` (group `snmp-worker`, offset `latest`) ใน Socket.IO background task
5. worker เก็บ 60 จุดล่าสุดและ emit event `traffic` ให้ทุก browser
6. browser ที่เชื่อมต่อใหม่ได้รับ event `history` (60 จุดล่าสุด) แล้วอัปเดตกราฟ Chart.js แบบ realtime

## Firewall

ตอนทดสอบ `ufw` อยู่ในสถานะ `inactive` จึงไม่ต้องเปิดพอร์ต หากเปิดใช้งานภายหลังต้องเปิดพอร์ต:

```bash
sudo ufw allow 5000/tcp   # กราฟ realtime
sudo ufw allow 8082/tcp   # kafka-ui
sudo ufw allow 9092/tcp   # Kafka (จำเป็นเมื่อ client อยู่นอก server)
```

## ผลการทดสอบ

ทดสอบเมื่อ 2026-10-21 บน Ubuntu 26.04 (`192.168.80.131`)

- SNMP walk ไป R1 `192.168.80.135`: ผ่าน, `Fa1/1` = ifIndex `3`
- Kafka: ใช้ container เดิม `apache/kafka:4.3.1`, topic `snmp` (1 partition, RF 1)
- รัน collector + worker พร้อมกัน: มี message ใน topic `snmp` 122 ข้อความ
- `http://192.168.80.131:5000`: HTTP 200 และ Socket.IO handshake ผ่าน (จากเครื่อง Windows)
- `http://192.168.80.131:8082` (kafka-ui): HTTP 200
- ufw: inactive
- Python packages: confluent-kafka 2.16.0, Flask 3.1.3, Flask-SocketIO 5.7.0, simple-websocket 1.1.0
- ตอนทดสอบยังไม่ได้รัน iperf3 ค่า IN/OUT จึงเป็น 0.00 Mbps

## ข้อควรระวัง

Kafka และ kafka-ui ในโปรเจกต์นี้ใช้ PLAINTEXT ไม่มี authentication และ SNMP v2c ส่ง community string แบบไม่เข้ารหัส จึงควรใช้เฉพาะใน Lab หรือเครือข่ายที่เชื่อถือได้ หากนำไปใช้งานจริงควรใช้ SNMPv3, Kafka SASL/TLS, เพิ่ม authentication ให้หน้าเว็บ และใช้ production WSGI server แทน Werkzeug

## Prompt สำหรับสั่ง AI ให้ติดตั้งและรัน

```text
ช่วยติดตั้งและรันโปรเจกต์ SNMP Kafka Realtime Traffic Graph บน Ubuntu Server ใน ~/snmp-kafka-project ให้ครบถ้วน โดยทำงานตามลำดับดังนี้:

1. อ่าน README.md, config.py, collector.py, worker.py, docker-compose.standalone.yml และ setup.sh เพื่อทำความเข้าใจระบบก่อนดำเนินการ
2. ตรวจสอบ IP ของ server ด้วย hostname -I และติดตั้ง snmp หากยังไม่มี
3. ทดสอบ snmpwalk -v2c -c public 192.168.80.135 1.3.6.1.2.1.31.1.1.1.1 และหา ifIndex ของ Fa1/1 หาก router ไม่ตอบให้หยุดและรายงาน
4. ตรวจสอบ Docker, compose plugin และ container Kafka/kafka-ui เดิม (port 9092, 8082) หากมีอยู่แล้วให้ใช้ของเดิม
5. หากยังไม่มี Kafka จึงรัน docker compose -f docker-compose.standalone.yml up -d แล้วสร้าง topic snmp (1 partition, RF 1, --if-not-exists)
6. สร้าง .venv และติดตั้ง requirements.txt
7. รัน worker.py และ collector.py พร้อมกันประมาณ 30 วินาที ตรวจว่ามี message ใน topic snmp และ http://SERVER_IP:5000 ตอบ HTTP 200
8. หาก ufw เปิดอยู่ ให้แจ้งพอร์ต 5000, 8082, 9092 ที่ต้องเปิด
9. ห้ามหยุดหรือลบ container, volume หรือไฟล์ที่มีอยู่เดิมโดยไม่ถาม
10. เมื่อเสร็จแล้วให้สรุป IF_INDEX, Kafka bootstrap, URL kafka-ui, URL กราฟ, คำสั่งรัน collector/worker และคำสั่ง start/stop Kafka
```
