"""
ingest - reads video (or RTSP) and publishes frames to Redis for the hot path
and to Kafka for the cold path when Kafka is available.
"""
import os
import time
import json
import base64
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
import cv2
import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

CAM_ID = os.environ["CAM_ID"]
SOURCE = os.environ["SOURCE"]
FPS = float(os.environ.get("FPS", "10"))
FRAME_SIZE = int(os.environ.get("FRAME_SIZE", "0"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "70"))
REDIS_MAXLEN = int(os.environ.get("REDIS_MAXLEN", "100"))
CV_THREADS = int(os.environ.get("CV_THREADS", "1"))
REDIS_HOST = os.environ.get("REDIS_HOST", "broker")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

REDIS_STREAM = f"frames:{CAM_ID}"
KAFKA_TOPIC = f"ice.frames.{CAM_ID}"
KAFKA_RETRY_SEC = 15

r = redis.Redis(host=REDIS_HOST, decode_responses=False)
producer = None
last_kafka_retry = 0.0

try:
    cv2.setNumThreads(CV_THREADS)
except Exception:
    pass


def connect_kafka(force=False):
    global producer, last_kafka_retry
    now = time.time()

    if producer is not None:
        return producer
    if not force and now - last_kafka_retry < KAFKA_RETRY_SEC:
        return None

    last_kafka_retry = now
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
        )
        print(f"[{CAM_ID}] Kafka producer ready", flush=True)
    except NoBrokersAvailable:
        print(f"[{CAM_ID}] Kafka unavailable, continuing with Redis-only hot path", flush=True)
        producer = None
    except Exception as e:
        print(f"[{CAM_ID}] Kafka connect error: {e}", flush=True)
        producer = None

    return producer


def send_to_kafka(payload):
    global producer
    kafka_producer = connect_kafka()
    if kafka_producer is None:
        return
    try:
        kafka_producer.send(KAFKA_TOPIC, payload)
    except Exception as e:
        print(f"[{CAM_ID}] Kafka send error: {e}", flush=True)
        try:
            kafka_producer.close()
        except Exception:
            pass
        producer = None


def encode_frame(frame):
    # FRAME_SIZE=0 keeps the native source resolution.
    if FRAME_SIZE > 0:
        frame = cv2.resize(frame, (FRAME_SIZE, FRAME_SIZE))
    _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    return base64.b64encode(buf.tobytes()).decode()


def open_capture():
    cap = cv2.VideoCapture(SOURCE)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open source: {SOURCE}")
    try:
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    except Exception:
        pass
    return cap


print(f"[{CAM_ID}] Connected: Redis={REDIS_HOST}, Kafka={KAFKA_BOOTSTRAP}", flush=True)
connect_kafka(force=True)

cap = open_capture()
interval = 1.0 / FPS
frame_count = 0
loop_count = 0

while True:
    ret, frame = cap.read()
    if not ret:
        loop_count += 1
        print(f"[{CAM_ID}] loop #{loop_count}, frames: {frame_count}", flush=True)
        cap.release()
        cap = open_capture()
        continue

    ts = time.time()
    b64 = encode_frame(frame)

    try:
        r.xadd(
            REDIS_STREAM,
            {"frame": b64, "ts": str(ts), "cam_id": CAM_ID},
            maxlen=REDIS_MAXLEN,
        )
    except Exception as e:
        print(f"[{CAM_ID}] Redis error: {e}", flush=True)

    send_to_kafka({"cam_id": CAM_ID, "ts": ts, "frame": b64})

    frame_count += 1
    if frame_count % 50 == 0:
        print(f"[{CAM_ID}] frames published: {frame_count}", flush=True)

    time.sleep(interval)
