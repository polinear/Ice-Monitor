"""
Inference worker that always processes the freshest frame from Redis.

The hot path should stay near real time even when the model is slower than the
incoming frame rate, so we intentionally drop stale frames instead of building
an ever-growing backlog.
"""
import os
import json
import time
import base64
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
import numpy as np
import cv2
try:
    cv2.setNumThreads(int(os.environ.get("CV_THREADS", "1")))
except Exception:
    pass
try:
    import torch
    torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", "1")))
    torch.set_num_interop_threads(int(os.environ.get("TORCH_INTEROP_THREADS", "1")))
except Exception:
    pass
import redis
from ultralytics import YOLO
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

CAM_ID = os.environ["CAM_ID"]
MODEL_PATH = os.environ["MODEL_PATH"]
REDIS_HOST = os.environ.get("REDIS_HOST", "broker")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INFER_SIZE = int(os.environ.get("INFER_SIZE", "224"))
CONF_THRESH = float(os.environ.get("CONF_THRESH", "0.08"))
IOU_THRESH = float(os.environ.get("IOU_THRESH", "0.45"))
FALLBACK_MODEL_PATH = os.environ.get("FALLBACK_MODEL_PATH")
SKIP_MODEL_INFERENCE = os.environ.get("SKIP_MODEL_INFERENCE", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ENABLE_FALLBACK_MODEL = os.environ.get("ENABLE_FALLBACK_MODEL", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
RETINA_MASKS = os.environ.get("RETINA_MASKS", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MODEL_INPUT_FLIP_HORIZONTAL = os.environ.get("MODEL_INPUT_FLIP_HORIZONTAL", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MAX_DET = int(os.environ.get("MAX_DET", "12"))
HEURISTIC_SIZE = int(os.environ.get("HEURISTIC_SIZE", "640"))
MODEL_MAX_SIDE = int(os.environ.get("MODEL_MAX_SIDE", "512"))

IN_STREAM = f"frames:{CAM_ID}"
OUT_STREAM = f"results:{CAM_ID}"
KAFKA_TOPIC = "ice.detections"
KAFKA_RETRY_SEC = 15

MODEL_CLASS_NAMES = {
    0: "slush_ice",
    1: "open_water",
    2: "broken_ice",
    3: "vessel",
    4: "ice_field",
    5: "background",
}
TRACKED_CLASS_NAMES = {"slush_ice", "broken_ice", "ice_field", "vessel"}
ICE_MASK_VALUES = {
    "vessel": 4,
    "ice_field": 1,
    "broken_ice": 2,
    "slush_ice": 3,
}
VESSEL_EXCLUDE_POLYGONS = {
    # Camera A: vessel is on the right side of the frame.
    "cam_a": [
        (0.38, 0.00),
        (1.00, 0.00),
        (1.00, 1.00),
        (0.64, 1.00),
        (0.46, 0.53),
    ],
    # Camera B: vessel is on the left side of the frame.
    "cam_b": [
        (0.00, 0.00),
        (0.58, 0.00),
        (0.46, 0.55),
        (0.56, 1.00),
        (0.00, 1.00),
    ],
}

r = redis.Redis(host=REDIS_HOST, decode_responses=False)
producer = None
fallback_model = None
last_kafka_retry = 0.0

def load_model(model_path, label):
    print(f"[{CAM_ID}] Loading {label} model: {model_path}", flush=True)
    inference_model = YOLO(model_path)
    print(
        f"[{CAM_ID}] {label.capitalize()} model loaded. "
        f"Model classes: {inference_model.names}",
        flush=True,
    )

    warmup_frame = np.zeros((INFER_SIZE, INFER_SIZE, 3), dtype=np.uint8)
    warmup_started = time.time()
    inference_model.predict(
        warmup_frame,
        imgsz=INFER_SIZE,
        device="cpu",
        verbose=False,
        retina_masks=RETINA_MASKS,
        max_det=MAX_DET,
    )
    print(
        f"[{CAM_ID}] {label.capitalize()} warm-up complete in "
        f"{time.time() - warmup_started:.2f}s (imgsz={INFER_SIZE})",
        flush=True,
    )
    return inference_model


def get_fallback_model():
    global fallback_model

    if SKIP_MODEL_INFERENCE or not ENABLE_FALLBACK_MODEL:
        return None
    if not FALLBACK_MODEL_PATH or FALLBACK_MODEL_PATH == MODEL_PATH:
        return None
    if fallback_model is None:
        fallback_model = load_model(FALLBACK_MODEL_PATH, "fallback")
    return fallback_model


model = None if SKIP_MODEL_INFERENCE else load_model(MODEL_PATH, "primary")


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


def decode_frame(b64_bytes):
    buf = np.frombuffer(base64.b64decode(b64_bytes), np.uint8)
    return cv2.imdecode(buf, cv2.IMREAD_COLOR)


def encode_frame(frame_np):
    # Keep the original resolution; only compress for transport.
    _, buf = cv2.imencode(".jpg", frame_np, [cv2.IMWRITE_JPEG_QUALITY, 85])
    return base64.b64encode(buf).decode()


def encode_mask(mask_np):
    _, buf = cv2.imencode(".png", mask_np)
    return base64.b64encode(buf).decode()


def build_vessel_exclusion_mask(cam_id: str, h: int, w: int) -> np.ndarray:
    """Build a camera-specific mask for the ship area.

    The ship sits on one side of the frame for this installation, so we carve
    out a slanted exclusion band from the ice analytics instead of letting the
    model treat vessel pixels as ice.
    """

    polygon = VESSEL_EXCLUDE_POLYGONS.get(cam_id)
    if polygon is not None:
        pts = np.array(
            [
                [int(px * w), int(py * h)]
                for px, py in polygon
            ],
            dtype=np.int32,
        )
        mask = np.zeros((h, w), dtype=np.uint8)
        cv2.fillPoly(mask, [pts], 1)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (111, 111))
        mask = cv2.dilate(mask, kernel, iterations=1)
        return mask

    cfg = None
    if cfg is None:
        return np.zeros((h, w), dtype=np.uint8)
    return np.zeros((h, w), dtype=np.uint8)


def solidify_binary_mask(mask: np.ndarray, *, close_kernel: tuple[int, int] = (11, 11)) -> np.ndarray:
    binary = (mask > 0).astype(np.uint8)
    if not np.any(binary):
        return binary

    contours, _ = cv2.findContours(binary * 255, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    filled = np.zeros_like(binary, dtype=np.uint8)
    if contours:
        cv2.drawContours(filled, contours, -1, 1, thickness=cv2.FILLED)
    else:
        filled = binary

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, close_kernel)
    filled = cv2.morphologyEx(filled, cv2.MORPH_CLOSE, kernel, iterations=1)
    return (filled > 0).astype(np.uint8)


def resize_to_max_side(frame: np.ndarray, max_side: int) -> np.ndarray:
    if max_side <= 0:
        return frame
    h, w = frame.shape[:2]
    longest = max(h, w)
    if longest <= max_side:
        return frame
    scale = max_side / float(longest)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    return cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)


def bbox_from_mask(mask: np.ndarray) -> list[int] | None:
    ys, xs = np.where(mask > 0)
    if xs.size == 0 or ys.size == 0:
        return None
    return [int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())]


def split_surface_masks(frame: np.ndarray, vessel_mask: np.ndarray) -> tuple[np.ndarray, np.ndarray] | None:
    h, w = frame.shape[:2]
    hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
    lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)

    candidate = (
        (hsv[:, :, 2] > 72)
        & (hsv[:, :, 1] < 190)
        & ~vessel_mask
    )
    candidate[: int(h * 0.18), :] = False
    candidate[int(h * 0.97) :, :] = False

    if not np.any(candidate):
        return None

    score = lab[:, :, 2].astype(np.float32) - 0.18 * hsv[:, :, 1].astype(np.float32)
    valid_scores = score[candidate]
    split = float(np.median(valid_scores))
    ice = candidate & (score <= split)
    slush = candidate & (score > split)

    min_pixels = max(800, int(h * w * 0.01))
    if ice.sum() < min_pixels // 4 or slush.sum() < min_pixels // 4:
        low = float(np.percentile(valid_scores, 40))
        high = float(np.percentile(valid_scores, 60))
        ice = candidate & (score <= low)
        slush = candidate & (score >= high)

    if ice.sum() < min_pixels // 4 and slush.sum() < min_pixels // 4:
        if CAM_ID == "cam_a":
            boundary = int(w * 0.56)
            ice = candidate & (np.indices((h, w))[1] < boundary)
            slush = candidate & ~ice
        else:
            boundary = int(w * 0.44)
            ice = candidate & (np.indices((h, w))[1] > boundary)
            slush = candidate & ~ice

    ice = solidify_binary_mask(ice, close_kernel=(17, 17))
    slush = solidify_binary_mask(slush, close_kernel=(15, 15))
    ice[vessel_mask] = 0
    slush[vessel_mask] = 0

    if ice.sum() < min_pixels and slush.sum() < min_pixels:
        return None
    return ice, slush


def restore_result_orientation(result, width: int, flip_horizontal: bool):
    if not flip_horizontal:
        return result

    restored = dict(result)

    try:
        mask = cv2.imdecode(
            np.frombuffer(base64.b64decode(result["mask_b64"]), np.uint8),
            cv2.IMREAD_GRAYSCALE,
        )
        if mask is not None:
            restored["mask_b64"] = encode_mask(cv2.flip(mask, 1))
    except Exception:
        pass

    detections = []
    for det in result.get("detections", []):
        det_copy = dict(det)
        bbox = det_copy.get("bbox")
        if bbox and len(bbox) == 4:
            x1, y1, x2, y2 = [int(v) for v in bbox]
            det_copy["bbox"] = [
                max(0, min(width, width - x2)),
                y1,
                max(0, min(width, width - x1)),
                y2,
            ]
        detections.append(det_copy)
    restored["detections"] = detections

    return restored


def process_frame(frame, ts, inference_model, model_label, *, frame_seq=None, source_ts=None, orig_shape=None):
    analysis_h, analysis_w = frame.shape[:2]
    orig_h, orig_w = orig_shape if orig_shape is not None else (analysis_h, analysis_w)
    combined_mask = np.zeros((analysis_h, analysis_w), dtype=np.uint8)
    class_pixels = {name: 0 for name in MODEL_CLASS_NAMES.values()}
    detections = []
    vessel_mask = np.zeros((analysis_h, analysis_w), dtype=bool)

    process_started = time.perf_counter()
    results = inference_model.predict(
        frame,
        imgsz=INFER_SIZE,
        device="cpu",
        conf=CONF_THRESH,
        iou=IOU_THRESH,
        retina_masks=RETINA_MASKS,
        max_det=MAX_DET,
        verbose=False,
    )[0]

    area_scale = (orig_h * orig_w) / float(max(1, analysis_h * analysis_w))
    x_scale = orig_w / float(analysis_w)
    y_scale = orig_h / float(analysis_h)

    if results.masks is not None and results.boxes is not None:
        masks = results.masks.data.cpu().numpy()
        boxes = results.boxes.xyxy.cpu().numpy()
        classes = results.boxes.cls.cpu().numpy().astype(int)
        confs = results.boxes.conf.cpu().numpy()

        for seg_mask, box, cls_id, conf in zip(masks, boxes, classes, confs):
            cls_id = int(cls_id)
            name = MODEL_CLASS_NAMES.get(cls_id)
            if name is None or name not in TRACKED_CLASS_NAMES:
                continue

            mask_np = (seg_mask > 0.5).astype(np.uint8)
            if mask_np.shape[:2] != (analysis_h, analysis_w):
                mask_np = cv2.resize(mask_np, (analysis_w, analysis_h), interpolation=cv2.INTER_NEAREST)
            area = int(round(mask_np.sum() * area_scale))
            if name == "vessel":
                vessel_mask |= mask_np > 0
                class_pixels[name] += area
                continue

            mask_value = ICE_MASK_VALUES.get(name)
            if mask_value is not None:
                combined_mask[mask_np > 0] = mask_value
            class_pixels[name] += area

            x1 = int(round(box[0] * x_scale))
            y1 = int(round(box[1] * y_scale))
            x2 = int(round(box[2] * x_scale))
            y2 = int(round(box[3] * y_scale))
            detections.append(
                {
                    "class_name": name,
                    "confidence": float(conf),
                    "area_pixels": area,
                    "bbox": [x1, y1, x2, y2],
                }
            )

    vessel_pixels = int(round(vessel_mask.sum() * area_scale))
    total = orig_h * orig_w
    valid_area = max(1, total - vessel_pixels)
    ice_total = class_pixels["ice_field"] + class_pixels["broken_ice"] + class_pixels["slush_ice"]
    ice_conc = ice_total / valid_area if valid_area else 0.0
    ice_severity = 0.0
    if ice_total > 0:
        weighted = class_pixels["broken_ice"] * 0.65 + class_pixels["slush_ice"] * 0.85
        ice_severity = weighted / ice_total
    class_pixels["vessel"] = vessel_pixels
    vessel_present = vessel_pixels > 0
    processed_ts = time.time()
    inference_ms = (time.perf_counter() - process_started) * 1000.0

    if (orig_h, orig_w) != (analysis_h, analysis_w):
        combined_mask = cv2.resize(combined_mask, (orig_w, orig_h), interpolation=cv2.INTER_NEAREST)

    return {
        "ts": ts,
        "source_ts": source_ts,
        "frame_seq": frame_seq,
        "processed_ts": processed_ts,
        "inference_ms": round(inference_ms, 3),
        "queue_age_ms": round((processed_ts - source_ts) * 1000.0, 3) if source_ts is not None else None,
        "cam_id": CAM_ID,
        "mask_b64": encode_mask(combined_mask),
        "ice_conc": round(ice_conc, 4),
        "ice_severity": round(ice_severity, 4),
        "vessel_present": vessel_present,
        "pixels": class_pixels,
        "detections": detections,
        "model_label": model_label,
        "class_names": MODEL_CLASS_NAMES,
    }


def process_heuristic_frame(frame, ts, *, frame_seq=None, source_ts=None, orig_shape=None):
    """Fallback segmentation for demo scenes when the model returns nothing."""
    process_started = time.perf_counter()
    orig_h, orig_w = orig_shape if orig_shape is not None else frame.shape[:2]
    analysis_frame = frame
    if HEURISTIC_SIZE > 0:
        analysis_frame = resize_to_max_side(frame, HEURISTIC_SIZE)

    h, w = analysis_frame.shape[:2]
    vessel_mask = build_vessel_exclusion_mask(CAM_ID, h, w).astype(bool)
    split_masks = split_surface_masks(analysis_frame, vessel_mask)
    combined_mask = np.zeros((h, w), dtype=np.uint8)

    if split_masks is not None:
        ice_mask, slush_mask = split_masks
        combined_mask[ice_mask > 0] = ICE_MASK_VALUES["ice_field"]
        combined_mask[slush_mask > 0] = ICE_MASK_VALUES["slush_ice"]
    else:
        # Last-resort demo fallback: stable coarse regions split by camera side.
        ice_side = np.zeros((h, w), dtype=bool)
        xx = np.indices((h, w))[1]
        if CAM_ID == "cam_a":
            ice_side |= xx < int(w * 0.58)
            slush_side = ~ice_side
        else:
            ice_side |= xx > int(w * 0.42)
            slush_side = ~ice_side
        ice_side[: int(h * 0.18), :] = False
        slush_side[: int(h * 0.18), :] = False
        ice_side[int(h * 0.97) :, :] = False
        slush_side[int(h * 0.97) :, :] = False
        combined_mask[ice_side & ~vessel_mask] = ICE_MASK_VALUES["ice_field"]
        combined_mask[slush_side & ~vessel_mask] = ICE_MASK_VALUES["slush_ice"]

    class_pixels = {name: 0 for name in MODEL_CLASS_NAMES.values()}
    area_scale = (orig_h * orig_w) / float(max(1, h * w))
    class_pixels["ice_field"] = int(round((combined_mask == ICE_MASK_VALUES["ice_field"]).sum() * area_scale))
    class_pixels["broken_ice"] = int(round((combined_mask == ICE_MASK_VALUES["broken_ice"]).sum() * area_scale))
    class_pixels["slush_ice"] = int(round((combined_mask == ICE_MASK_VALUES["slush_ice"]).sum() * area_scale))
    vessel_pixels = int(round(vessel_mask.sum() * area_scale))
    class_pixels["vessel"] = vessel_pixels
    ice_total = class_pixels["ice_field"] + class_pixels["broken_ice"] + class_pixels["slush_ice"]
    total = orig_h * orig_w
    valid_area = max(1, total - vessel_pixels)
    processed_ts = time.time()
    inference_ms = (time.perf_counter() - process_started) * 1000.0

    detections = []
    for class_name, mask_value in (("ice_field", ICE_MASK_VALUES["ice_field"]), ("broken_ice", ICE_MASK_VALUES["broken_ice"]), ("slush_ice", ICE_MASK_VALUES["slush_ice"])):
        mask = combined_mask == mask_value
        if not np.any(mask):
            continue
        bbox = bbox_from_mask(mask)
        if bbox is None:
            continue
        bbox = [
            int(round(bbox[0] * (orig_w / float(w)))),
            int(round(bbox[1] * (orig_h / float(h)))),
            int(round(bbox[2] * (orig_w / float(w)))),
            int(round(bbox[3] * (orig_h / float(h)))),
        ]
        detections.append(
            {
                "class_name": class_name,
                "confidence": 0.35 if class_name == "ice_field" else 0.28,
                "area_pixels": int(round(mask.sum() * area_scale)),
                "bbox": bbox,
            }
        )

    if (orig_h, orig_w) != (h, w):
        combined_mask = cv2.resize(combined_mask, (orig_w, orig_h), interpolation=cv2.INTER_NEAREST)

    return {
        "ts": ts,
        "source_ts": source_ts,
        "frame_seq": frame_seq,
        "processed_ts": processed_ts,
        "inference_ms": round(inference_ms, 3),
        "queue_age_ms": round((processed_ts - source_ts) * 1000.0, 3) if source_ts is not None else None,
        "cam_id": CAM_ID,
        "mask_b64": encode_mask(combined_mask),
        "ice_conc": round(ice_total / valid_area, 4) if valid_area else 0.0,
        "ice_severity": round((class_pixels["broken_ice"] * 0.65 + class_pixels["slush_ice"] * 0.85) / ice_total, 4) if ice_total else 0.0,
        "vessel_present": vessel_pixels > 0,
        "pixels": class_pixels,
        "detections": detections,
        "model_label": "heuristic",
        "class_names": MODEL_CLASS_NAMES,
    }


print(f"[{CAM_ID}] Worker ready", flush=True)
connect_kafka(force=True)

last_msg_id = None
published_results = 0

while True:
    try:
        latest_entries = r.xrevrange(IN_STREAM, count=1)
        if not latest_entries:
            time.sleep(0.2)
            continue

        msg_id, data = latest_entries[0]
        if msg_id == last_msg_id:
            time.sleep(0.05)
            continue

        last_msg_id = msg_id
        frame = decode_frame(data[b"frame"])
        ts = float(data[b"ts"].decode())
        frame_seq_raw = data.get(b"frame_seq")
        frame_seq = int(frame_seq_raw.decode()) if frame_seq_raw is not None and frame_seq_raw != b"" else None
        frame_for_model = cv2.flip(frame, 1) if MODEL_INPUT_FLIP_HORIZONTAL else frame
        orig_shape = frame_for_model.shape[:2]
        model_frame = resize_to_max_side(frame_for_model, MODEL_MAX_SIDE)
        if SKIP_MODEL_INFERENCE:
            result = process_heuristic_frame(
                frame_for_model,
                ts,
                frame_seq=frame_seq,
                source_ts=ts,
                orig_shape=orig_shape,
            )
            if result["detections"]:
                print(
                    f"[{CAM_ID}] heuristic mask used "
                    f"(ice_pixels={result['pixels']['ice_field'] + result['pixels']['broken_ice'] + result['pixels']['slush_ice']})",
                    flush=True,
                )
        else:
            result = process_frame(
                model_frame,
                ts,
                model,
                "primary",
                frame_seq=frame_seq,
                source_ts=ts,
                orig_shape=orig_shape,
            )

        result = restore_result_orientation(result, frame.shape[1], MODEL_INPUT_FLIP_HORIZONTAL)

        now = time.time()
        r.xadd(
            OUT_STREAM,
            {
                "cam_id": result["cam_id"],
                "ts": str(result["ts"]),
                "source_ts": str(result["source_ts"]) if result.get("source_ts") is not None else "",
                "frame_seq": str(result["frame_seq"]) if result.get("frame_seq") is not None else "",
                "processed_ts": str(result["processed_ts"]),
                "inference_ms": str(result["inference_ms"]),
                "queue_age_ms": str(result["queue_age_ms"]) if result.get("queue_age_ms") is not None else "",
                "mask": result["mask_b64"],
                "ice_conc": str(result["ice_conc"]),
                "ice_severity": str(result["ice_severity"]),
                "vessel_present": "1" if result["vessel_present"] else "0",
                "pixels_json": json.dumps(result["pixels"]),
                "class_names": json.dumps(result["class_names"]),
            },
            maxlen=200,
        )
        published_results += 1
        if published_results % 5 == 0:
            print(
                f"[{CAM_ID}] results published: {published_results} "
                f"(model={result['model_label']}, "
                f"detections={len(result['detections'])}, "
                f"ice_conc={result['ice_conc']}, vessel={result['vessel_present']})",
                flush=True,
            )

        send_to_kafka(
            {
                "ts": result["ts"],
                "source_ts": result.get("source_ts"),
                "frame_seq": result.get("frame_seq"),
                "processed_ts": result.get("processed_ts"),
                "inference_ms": result.get("inference_ms"),
                "queue_age_ms": result.get("queue_age_ms"),
                "cam_id": result["cam_id"],
                "ice_conc": result["ice_conc"],
                "ice_severity": result["ice_severity"],
                "vessel_present": result["vessel_present"],
                "pixels": result["pixels"],
                "detections": result["detections"],
            }
        )
    except Exception as e:
        print(f"[{CAM_ID}] Processing error: {e}", flush=True)
        time.sleep(1)
