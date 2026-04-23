"""Modern operator dashboard for the Ice Monitor demo."""
import base64
import html
import json
import os
import time
from textwrap import dedent
from collections import deque
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Lock, Thread
from zoneinfo import ZoneInfo

import cv2
import numpy as np
import requests
import streamlit as st
import websocket

# ---------------------------------------------------------------------------
# РљРѕРЅС„РёРіСѓСЂР°С†РёСЏ
# ---------------------------------------------------------------------------
API_HOST = os.environ.get("API_HOST", "api")
API_URL = f"http://{API_HOST}:8000"
FRAME_WS_URL = f"ws://{API_HOST}:8000/stream/frames"
RESULT_WS_URL = f"ws://{API_HOST}:8000/stream/results"

CAMERAS = [
    ("cam_a", "Камера 1"),
    ("cam_b", "Камера 2"),
]
LOCAL_TZ = ZoneInfo("Europe/Moscow")

STATUS_REFRESH_SEC = 3
SYSTEM_REFRESH_SEC = 5
LOG_REFRESH_SEC = 4
HISTORY_REFRESH_SEC = 6
RENDER_INTERVAL_SEC = 0.08
DISPLAY_MAX_WIDTH = 680
DISPLAY_MAX_HEIGHT = 382
MAX_LOG_ROWS = 8
TREND_POINTS = 60
MAX_MASK_AGE_SEC = 8.0
LIVE_METRIC_WINDOW = 12
LIVE_METRIC_TREND_ALPHA = 0.35
LIVE_METRIC_FRESHNESS_HALF_LIFE_SEC = 8.0

# Tracked classes are shown in the overlay; open water/background are muted
# because the demo does not score them.
MASK_HUD_LABELS = {
    1: "ICE FIELD",
    2: "BROKEN",
    3: "SLUSH",
}

MASK_CLASSES = {
    1: {
        "label": "Сплошной лёд",
        "fill": (234, 249, 255),
        "edge": (255, 255, 255),
        "alpha": 0.48,
        "hex": "#eaf9ff",
    },
    2: {
        "label": "Битый лёд",
        "fill": (183, 224, 242),
        "edge": (214, 239, 250),
        "alpha": 0.56,
        "hex": "#b7e0f2",
    },
    3: {
        "label": "Шуга",
        "fill": (127, 184, 217),
        "edge": (163, 212, 235),
        "alpha": 0.62,
        "hex": "#7fb8d9",
    },
}

VESSEL_EXCLUDE_POLYGONS = {
    # Narrow static strips that follow the ship side, not the surrounding ice.
    "cam_a": [(0.78, 0.00), (0.96, 0.00), (0.99, 0.55), (0.90, 1.00), (0.76, 1.00)],
    "cam_b": [(0.00, 0.00), (0.16, 0.00), (0.22, 0.55), (0.14, 1.00), (0.00, 1.00)],
}

# Slightly widen the strip so the static hull boundary does not leak into ice KPIs.
VESSEL_EXCLUDE_DILATION = {
    "cam_a": 23,
    "cam_b": 27,
}

SERVICE_LABELS = {
    "api": "API",
    "frontend": "Интерфейс",
    "redis": "Redis",
    "kafka": "Kafka",
    "timescaledb": "TimescaleDB",
    "namenode": "HDFS",
    "spark_master": "Spark",
    "hive_server": "Hive",
    "superset": "Superset",
}

CORE_SERVICE_ORDER = [
    "api",
    "redis",
    "kafka",
    "timescaledb",
    "namenode",
    "spark_master",
    "hive_server",
    "superset",
]

LOG_LEVEL_CLASS = {
    "info": "neutral",
    "warning": "warning",
    "error": "danger",
}

LOG_LEVEL_LABEL = {
    "info": "Инфо",
    "warning": "Предупр.",
    "error": "Ошибка",
}


# ---------------------------------------------------------------------------
# Streamlit-СЃС‚СЂР°РЅРёС†Р° Рё СЃС‚РёР»СЊ
# ---------------------------------------------------------------------------
st.set_page_config(
    layout="wide",
    page_title="Ice Monitor — панель оператора",
    page_icon="🧊",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    :root {
        --bg0: #070d17;
        --bg1: #0b1524;
        --bg2: #0f1d31;
        --panel: rgba(16, 28, 47, 0.78);
        --panel-hi: rgba(26, 44, 72, 0.85);
        --line: rgba(148, 184, 226, 0.14);
        --line-strong: rgba(148, 184, 226, 0.28);
        --text: #e8f1ff;
        --muted: #8ea4c4;
        --accent: #5ec8ff;
        --accent-2: #7effd9;
        --amber: #ffc870;
        --red: #ff8a8a;
        --green: #7bf0b8;
        --shadow-lg: 0 24px 56px rgba(0, 0, 0, 0.5);
        --shadow-md: 0 10px 24px rgba(0, 0, 0, 0.38);
    }

    html, body, [class*="css"] {
        font-family: "Inter", "Segoe UI", -apple-system, BlinkMacSystemFont,
                     "Helvetica Neue", sans-serif;
        -webkit-font-smoothing: antialiased;
    }

    .stApp {
        background:
            radial-gradient(1100px 650px at 82% -4%, rgba(94, 200, 255, 0.14), transparent 60%),
            radial-gradient(900px 560px at -10% 12%, rgba(126, 255, 217, 0.08), transparent 55%),
            radial-gradient(800px 500px at 50% 110%, rgba(148, 120, 255, 0.10), transparent 60%),
            linear-gradient(180deg, #05090f 0%, #0a1423 48%, #060b14 100%);
        color: var(--text);
    }

    #MainMenu, header, footer {visibility: hidden;}

    [data-testid="stAppViewContainer"] > .main {background: transparent;}

    [data-testid="block-container"] {
        max-width: 116rem;
        padding: 0.5rem 1rem 0.6rem 1rem;
    }

    [data-testid="stImage"] {
        width: 100%;
        max-width: 430px;
        margin: 0 auto;
    }

    [data-testid="stImage"] img {
        border-radius: 18px;
        border: 1px solid var(--line-strong);
        box-shadow: var(--shadow-lg);
        width: 100% !important;
        height: auto !important;
        max-width: 100% !important;
        object-fit: contain;
        display: block;
    }

    [data-testid="stMarkdownContainer"] p {margin-bottom: 0;}

    /* ---------- РљР°СЂС‚РѕС‡РєРё ---------- */
    .card {
        background: linear-gradient(165deg, rgba(18, 32, 54, 0.88), rgba(10, 20, 36, 0.92));
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: var(--shadow-md);
        padding: 1rem 1.15rem;
        backdrop-filter: blur(12px);
    }

    .card + .card {margin-top: 0.7rem;}

    .card-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.6rem;
        margin-bottom: 0.75rem;
    }

    .eyebrow {
        color: var(--muted);
        font-size: 0.66rem;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        font-weight: 600;
    }

    /* ---------- РЁР°РїРєР° ---------- */
    .hero {
        display: grid;
        grid-template-columns: 1fr;
        gap: 0.8rem;
        align-items: stretch;
        margin-bottom: 0.75rem;
    }

    .hero-main {
        background:
            radial-gradient(500px 260px at 0% 0%, rgba(94, 200, 255, 0.24), transparent 70%),
            radial-gradient(400px 240px at 100% 100%, rgba(126, 255, 217, 0.18), transparent 75%),
            linear-gradient(135deg, rgba(18, 32, 54, 0.95), rgba(10, 20, 36, 0.98));
        border: 1px solid var(--line-strong);
        border-radius: 20px;
        box-shadow: var(--shadow-lg);
        padding: 0.9rem 1.15rem;
        display: flex;
        flex-direction: column;
        gap: 0.4rem;
    }

    .hero-title {
        font-size: 1.18rem;
        font-weight: 700;
        letter-spacing: 0.01em;
        display: flex;
        align-items: center;
        gap: 0.55rem;
    }

    .hero-logo {
        width: 30px;
        height: 30px;
        border-radius: 10px;
        background: linear-gradient(140deg, var(--accent), var(--accent-2));
        box-shadow: 0 8px 24px rgba(94, 200, 255, 0.45);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.92rem;
    }

    .hero-sub {
        color: var(--muted);
        font-size: 0.82rem;
        line-height: 1.45;
        max-width: 72ch;
    }

    .hero-sub strong {
        color: var(--text);
        font-weight: 700;
    }

    .hero-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
        margin-top: 0.2rem;
    }

    .hero-stats {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.55rem;
    }

    .stat-card {
        background: linear-gradient(160deg, rgba(16, 28, 48, 0.92), rgba(10, 20, 36, 0.96));
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 0.75rem 0.85rem;
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
        min-height: 5.4rem;
        position: relative;
        overflow: hidden;
    }

    .stat-card::before {
        content: "";
        position: absolute;
        inset: 0 0 auto 0;
        height: 3px;
        background: linear-gradient(90deg, var(--accent), var(--accent-2));
        opacity: 0.75;
    }

    .stat-label {
        color: var(--muted);
        font-size: 0.64rem;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        font-weight: 600;
    }

    .stat-value {
        font-size: 1.32rem;
        font-weight: 700;
        letter-spacing: -0.01em;
    }

    .stat-hint {color: var(--muted); font-size: 0.7rem;}

    .stat-card.danger::before {background: linear-gradient(90deg, var(--red), var(--amber));}
    .stat-card.caution::before {background: linear-gradient(90deg, var(--amber), var(--accent-2));}
    .stat-card.ok::before {background: linear-gradient(90deg, var(--accent-2), var(--green));}

    /* ---------- Р§РёРїС‹ ---------- */
    .chip {
        display: inline-flex;
        align-items: center;
        gap: 0.38rem;
        padding: 0.3rem 0.58rem;
        border-radius: 999px;
        border: 1px solid var(--line-strong);
        background: rgba(11, 22, 38, 0.75);
        color: var(--text);
        font-size: 0.72rem;
        font-weight: 500;
        white-space: nowrap;
    }

    .chip .dot {
        width: 0.46rem;
        height: 0.46rem;
        border-radius: 999px;
        background: var(--muted);
        box-shadow: 0 0 10px currentColor;
    }

    .chip.ok {color: var(--green); border-color: rgba(123, 240, 184, 0.35);}
    .chip.ok .dot {background: var(--green); color: var(--green);}
    .chip.warn {color: var(--amber); border-color: rgba(255, 200, 112, 0.35);}
    .chip.warn .dot {background: var(--amber); color: var(--amber);}
    .chip.bad {color: var(--red); border-color: rgba(255, 138, 138, 0.4);}
    .chip.bad .dot {background: var(--red); color: var(--red);}

    .chip-row {display: flex; flex-wrap: wrap; gap: 0.4rem;}

    /* ---------- РљР°РјРµСЂС‹ ---------- */
    .cam-card {
        background: linear-gradient(165deg, rgba(18, 32, 54, 0.92), rgba(10, 20, 36, 0.96));
        border: 1px solid var(--line);
        border-radius: 20px;
        padding: 0.5rem;
        box-shadow: var(--shadow-md);
    }

    .cam-card.danger {border-color: rgba(255, 138, 138, 0.4);}
    .cam-card.caution {border-color: rgba(255, 200, 112, 0.4);}

    .cam-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.5rem;
        margin-bottom: 0.6rem;
    }

    .cam-title {
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: 0.01em;
        display: flex;
        align-items: center;
        gap: 0.55rem;
    }

    .cam-dot {
        width: 0.55rem;
        height: 0.55rem;
        border-radius: 999px;
        background: var(--green);
        box-shadow: 0 0 12px var(--green);
        animation: pulse 2.2s infinite ease-in-out;
    }

    .cam-dot.warn {background: var(--amber); box-shadow: 0 0 12px var(--amber);}
    .cam-dot.bad {background: var(--red); box-shadow: 0 0 12px var(--red);}

    @keyframes pulse {
        0%, 100% {opacity: 0.45; transform: scale(0.9);}
        50% {opacity: 1; transform: scale(1.1);}
    }

    .cam-metrics {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.4rem;
        margin-top: 0.45rem;
    }

    .metric {
        background: rgba(6, 14, 26, 0.82);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 0.4rem 0.52rem;
    }

    .metric-label {
        color: var(--muted);
        font-size: 0.62rem;
        text-transform: uppercase;
        letter-spacing: 0.16em;
        margin-bottom: 0.28rem;
        font-weight: 600;
    }

    .metric-value {font-size: 1.08rem; font-weight: 700;}
    .metric-value.ok {color: var(--green);}
    .metric-value.warn {color: var(--amber);}
    .metric-value.bad {color: var(--red);}

    .metric-note {
        color: var(--muted);
        font-size: 0.68rem;
        line-height: 1.35;
        margin-top: 0.38rem;
    }

    .metric-bar {
        margin-top: 0.5rem;
        height: 5px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.06);
        overflow: hidden;
    }

    .metric-bar > span {
        display: block;
        height: 100%;
        border-radius: 999px;
        background: linear-gradient(90deg, var(--accent), var(--accent-2));
        transition: width 0.4s ease;
    }

    .metric-bar.danger > span {background: linear-gradient(90deg, var(--amber), var(--red));}
    .metric-bar.caution > span {background: linear-gradient(90deg, var(--accent-2), var(--amber));}

    .cam-foot {
        margin-top: 0.65rem;
        display: flex;
        justify-content: space-between;
        color: var(--muted);
        font-size: 0.72rem;
        font-family: "JetBrains Mono", "Cascadia Code", monospace;
    }

    /* ---------- Панели ---------- */
    .panel-card {
        background: linear-gradient(165deg, rgba(18, 32, 54, 0.88), rgba(10, 20, 36, 0.94));
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 0.78rem 0.9rem 0.85rem 0.9rem;
        box-shadow: var(--shadow-md);
    }

    .panel-title {
        color: var(--text);
        font-size: 0.92rem;
        font-weight: 700;
        margin-bottom: 0.65rem;
        letter-spacing: 0.01em;
    }

    .legend-grid,
    .service-list,
    .log-list {
        display: grid;
        gap: 0.38rem;
    }

    .legend-item,
    .service-item,
    .log-entry {
        display: grid;
        grid-template-columns: auto 1fr auto;
        gap: 0.55rem;
        align-items: center;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: rgba(8, 16, 30, 0.65);
        padding: 0.46rem 0.58rem;
    }

    .legend-swatch {
        width: 1rem;
        height: 1rem;
        border-radius: 5px;
        border: 1px solid rgba(255, 255, 255, 0.25);
        box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.03) inset;
    }

    .legend-label,
    .service-name {
        font-weight: 600;
        font-size: 0.78rem;
        line-height: 1.2;
    }

    .legend-sub {
        color: var(--muted);
        font-size: 0.68rem;
        margin-top: 0.05rem;
    }

    .legend-hex,
    .service-host,
    .log-time,
    .log-source,
    .log-message {
        color: var(--muted);
        font-size: 0.7rem;
    }

    .service-pill,
    .log-tag {
        justify-self: end;
        font-size: 0.64rem;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        font-weight: 700;
        padding: 0.16rem 0.38rem;
        border-radius: 999px;
        border: 1px solid var(--line);
        white-space: nowrap;
    }

    .service-pill.ok,
    .log-tag.info {
        color: var(--green);
        border-color: rgba(123, 240, 184, 0.35);
    }

    .service-pill.bad,
    .log-tag.warning {
        color: var(--amber);
        border-color: rgba(255, 200, 112, 0.4);
    }

    .log-tag.error {
        color: var(--red);
        border-color: rgba(255, 138, 138, 0.45);
    }

    .log-meta {
        display: flex;
        gap: 0.4rem;
        align-items: center;
        flex-wrap: wrap;
        margin-bottom: 0.14rem;
    }

    /* ---------- Trend svg ---------- */
    .trend-wrap {
        padding: 0.35rem 0.2rem 0 0.2rem;
    }

    .trend-title {
        color: var(--muted);
        font-size: 0.66rem;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }

    .trend-svg {
        width: 100%;
        height: 84px;
        display: block;
    }

    .trend-legend {
        display: flex;
        justify-content: space-between;
        color: var(--muted);
        font-size: 0.66rem;
        margin-top: 0.3rem;
    }

    .muted {color: var(--muted); font-size: 0.8rem;}

    .section-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, var(--line-strong), transparent);
        margin: 0.5rem 0 0.7rem 0;
    }

    .section-title {
        font-size: 0.78rem;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        font-weight: 700;
        color: var(--muted);
        margin: 0.05rem 0 0.2rem 0;
    }

    .section-subtitle {
        color: var(--muted);
        font-size: 0.82rem;
        line-height: 1.5;
        margin-bottom: 0.5rem;
    }

    @media (max-width: 1100px) {
        .hero {grid-template-columns: 1fr;}
        .hero-stats {grid-template-columns: repeat(2, minmax(0, 1fr));}
    }

    @media (max-width: 780px) {
        .cam-metrics {grid-template-columns: 1fr;}
        [data-testid="stImage"] img {max-height: none;}
    }
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# РЎРѕСЃС‚РѕСЏРЅРёРµ Рё WS-РІРѕСЂРєРµСЂС‹
# ---------------------------------------------------------------------------
class CamState:
    def __init__(self):
        self.frame_queue: Queue = Queue(maxsize=4)
        self.result_queue: Queue = Queue(maxsize=2)
        self.lock = Lock()
        self.last_frame = None
        self.last_mask = None
        self.last_frame_ts = None
        self.last_result_ts = None
        self.last_result_source_ts = None
        self.last_frame_at = None
        self.last_result_at = None
        self.last_metrics = {"ice_conc": 0.0, "ice_severity": 0.0}
        self.last_analysis = None
        self.last_vessel_present = False
        self.trend: deque = deque(maxlen=TREND_POINTS)
        self.last_render_key = None
        self.last_render_preview = None
        self.last_card_html = None


if "cam_states" not in st.session_state:
    st.session_state.cam_states = {cam_id: CamState() for cam_id, _ in CAMERAS}
if "ws_started" not in st.session_state:
    st.session_state.ws_started = False
if "started_at" not in st.session_state:
    st.session_state.started_at = time.time()
if "hero_second" not in st.session_state:
    st.session_state.hero_second = None


def decode_b64_image(b64_image: str, flags: int):
    if not b64_image:
        return None
    try:
        raw = base64.b64decode(b64_image)
        buf = np.frombuffer(raw, np.uint8)
        return cv2.imdecode(buf, flags)
    except Exception:
        return None


def frame_to_bgr(b64_frame: str):
    return decode_b64_image(b64_frame, cv2.IMREAD_COLOR)


def mask_to_gray(b64_mask: str):
    return decode_b64_image(b64_mask, cv2.IMREAD_GRAYSCALE)


def draw_badge(frame: np.ndarray, text: str, x: int, y: int):
    (width, height), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.48, 1)
    cv2.rectangle(frame, (x, y), (x + width + 18, y + height + 14), (10, 17, 24), -1)
    cv2.rectangle(frame, (x, y), (x + width + 18, y + height + 14), (62, 94, 119), 1)
    cv2.putText(
        frame,
        text,
        (x + 9, y + height + 5),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (232, 240, 247),
        1,
        cv2.LINE_AA,
    )


def build_vessel_mask(cam_id: str, shape: tuple[int, int]) -> np.ndarray:
    h, w = shape
    mask = np.zeros((h, w), dtype=np.uint8)
    polygon = VESSEL_EXCLUDE_POLYGONS.get(cam_id)
    if not polygon:
        return mask

    pts = np.array([[int(px * w), int(py * h)] for px, py in polygon], dtype=np.int32)
    cv2.fillPoly(mask, [pts], 1)
    dilation = int(VESSEL_EXCLUDE_DILATION.get(cam_id, 111))
    dilation = dilation if dilation % 2 == 1 else dilation + 1
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (dilation, dilation))
    mask = cv2.dilate(mask, kernel, iterations=1)
    return mask


def build_overlay(frame: np.ndarray | None, mask: np.ndarray | None, cam_id: str | None = None):
    if frame is None:
        return None
    if mask is None:
        return frame

    if frame.shape[:2] != mask.shape[:2]:
        mask = cv2.resize(mask, (frame.shape[1], frame.shape[0]), interpolation=cv2.INTER_NEAREST)

    if cam_id is not None:
        vessel_region = build_vessel_mask(cam_id, mask.shape[:2])
        if np.any(vessel_region):
            mask = mask.copy()
            mask[vessel_region > 0] = 0

    preview = frame.copy()
    active_labels = []
    for cls_id, meta in MASK_CLASSES.items():
        region = mask == cls_id
        if not np.any(region):
            continue

        active_labels.append(MASK_HUD_LABELS.get(cls_id, f"MASK {cls_id}"))
        fill = np.zeros_like(preview)
        # HTML legend uses RGB hex, while OpenCV draws in BGR.
        fill[:] = meta["fill"][::-1]
        preview[region] = cv2.addWeighted(
            preview[region],
            1.0 - meta["alpha"],
            fill[region],
            meta["alpha"],
            0,
        )

    draw_badge(preview, "ICE MASK", 16, 16)
    if active_labels:
        x = 126
        for label in active_labels[:3]:
            draw_badge(preview, label, x, 16)
            x += 148
    else:
        draw_badge(preview, "NO ICE", 126, 16)

    return preview


def parse_ts(value):
    if value in (None, "", 0):
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        text = str(value).strip()
        if text.replace(".", "", 1).isdigit():
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def format_ts(value):
    ts = parse_ts(value)
    if ts is None:
        return "нет"
    return ts.astimezone(LOCAL_TZ).strftime("%H:%M:%S")


def load_json(path: str, *, timeout: int = 2):
    try:
        response = requests.get(f"{API_URL}{path}", timeout=timeout)
        if not response.ok:
            return None, f"{path}: код {response.status_code}"
        return response.json(), None
    except Exception:
        return None, f"{path}: сервис недоступен"


def load_pipeline_status():
    payload, error = load_json("/pipeline_status")
    if error:
        return {}, error
    return {item["cam_id"]: item for item in payload.get("cameras", [])}, None


def load_system_status():
    payload, error = load_json("/system_status")
    return payload or {}, error


def load_logs():
    payload, error = load_json(f"/logs?limit={MAX_LOG_ROWS}")
    if error:
        return [], error
    return payload.get("entries", []), None


def queue_put_latest(queue: Queue, item):
    if queue.full():
        try:
            queue.get_nowait()
        except Empty:
            pass
    queue.put(item)


def drain_latest(queue: Queue):
    latest = None
    while True:
        try:
            latest = queue.get_nowait()
        except Empty:
            return latest


def ws_worker(url_prefix: str, cam_id: str, queue: Queue):
    url = f"{url_prefix}/{cam_id}"
    while True:
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=20)
            ws.settimeout(None)
            while True:
                message = ws.recv()
                if not message:
                    # Ignore transient empty frames instead of forcing a reconnect.
                    continue
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    continue
                queue_put_latest(queue, payload)
        except Exception as exc:
            print(f"[ws/{cam_id}] reconnect {url}: {exc}", flush=True)
            time.sleep(2)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def update_state_from_streams():
    for cam_id, _ in CAMERAS:
        state = st.session_state.cam_states[cam_id]

        frame_msg = drain_latest(state.frame_queue)
        if frame_msg:
            frame = frame_to_bgr(frame_msg.get("frame"))
            if frame is not None:
                with state.lock:
                    state.last_frame = frame
                    state.last_frame_ts = float(frame_msg["ts"])
                    state.last_frame_at = time.time()

        result_msg = drain_latest(state.result_queue)
        if result_msg:
            mask = mask_to_gray(result_msg.get("mask"))
            conc = float(result_msg.get("ice_conc", 0.0))
            sev = float(result_msg.get("ice_severity", 0.0))
            source_ts_raw = result_msg.get("source_ts")
            source_ts = float(source_ts_raw) if source_ts_raw not in (None, "") else float(result_msg["ts"])
            analysis = analyze_mask(mask, cam_id)
            with state.lock:
                state.last_mask = mask
                state.last_result_ts = float(result_msg["ts"])
                state.last_result_source_ts = source_ts
                state.last_result_at = time.time()
                state.last_metrics = {
                    "ice_conc": conc,
                    "ice_severity": sev,
                }
                state.last_analysis = analysis
                state.last_vessel_present = bool(analysis.get("vessel_present", result_msg.get("vessel_present", False)))
                state.trend.append((state.last_result_ts, float(analysis.get("field_cover", 0.0))))


def html_escape(value):
    return html.escape(str(value))


def chip(label: str, state: str = "neutral"):
    return (
        f"<span class='chip {state}'><span class='dot'></span>"
        f"{html_escape(label)}</span>"
    )


def clamp01(value: float) -> float:
    return max(0.0, min(value, 1.0))


def ema_value(values: list[float], alpha: float = LIVE_METRIC_TREND_ALPHA) -> float:
    if not values:
        return 0.0
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def analyze_mask(mask: np.ndarray | None, cam_id: str | None = None) -> dict[str, float | int | str | bool | None]:
    if mask is None:
        return {
            "total_pixels": 0,
            "usable_pixels": 0,
            "vessel_pixels": 0,
            "ice_field_pixels": 0,
            "broken_ice_pixels": 0,
            "slush_ice_pixels": 0,
            "ice_cover": 0.0,
            "field_cover": 0.0,
            "broken_cover": 0.0,
            "slush_cover": 0.0,
            "field_share": 0.0,
            "broken_share": 0.0,
            "slush_share": 0.0,
            "loose_share": 0.0,
            "roughness": 0.0,
            "scene_pressure": 0.0,
            "dominant_class": "open_water",
            "dominant_label": "Открытая вода",
            "scene_label": "Открытая вода",
            "vessel_present": False,
        }

    if cam_id is not None:
        vessel_region = build_vessel_mask(cam_id, mask.shape[:2])
        if np.any(vessel_region):
            mask = mask.copy()
            mask[vessel_region > 0] = 0

    counts = {
        cls_id: int(np.count_nonzero(mask == cls_id))
        for cls_id in (1, 2, 3, 4)
    }
    total_pixels = int(mask.size)
    vessel_pixels = counts[4]
    usable_pixels = max(1, total_pixels - vessel_pixels)
    ice_field_pixels = counts[1]
    broken_ice_pixels = counts[2]
    slush_ice_pixels = counts[3]
    ice_pixels = ice_field_pixels + broken_ice_pixels + slush_ice_pixels

    ice_cover = ice_pixels / usable_pixels
    field_cover = ice_field_pixels / usable_pixels
    broken_cover = broken_ice_pixels / usable_pixels
    slush_cover = slush_ice_pixels / usable_pixels

    if ice_pixels > 0:
        field_share = ice_field_pixels / ice_pixels
        broken_share = broken_ice_pixels / ice_pixels
        slush_share = slush_ice_pixels / ice_pixels
        loose_share = (broken_ice_pixels + slush_ice_pixels) / ice_pixels
        dominant_class = max(
            ((1, ice_field_pixels), (2, broken_ice_pixels), (3, slush_ice_pixels)),
            key=lambda item: item[1],
        )[0]
    else:
        field_share = broken_share = slush_share = loose_share = 0.0
        dominant_class = 0

    roughness = clamp01(0.7 * broken_share + 0.3 * slush_share)
    scene_pressure = clamp01(
        0.72 * field_cover
        + 0.18 * ice_cover
        + 0.10 * roughness
    )

    if ice_pixels == 0:
        scene_label = "Открытая вода"
        dominant_label = "Открытая вода"
    elif field_cover >= 0.35 or scene_pressure >= 0.45:
        scene_label = "Плотное ледовое поле"
        dominant_label = "Сплошной лёд"
    elif field_cover >= 0.16:
        scene_label = "Смешанный лёд"
        dominant_label = MASK_CLASSES.get(dominant_class, {}).get("label", "Лёд")
    elif loose_share >= 0.65:
        scene_label = "Рыхлый лёд"
        dominant_label = MASK_CLASSES.get(dominant_class, {}).get("label", "Лёд")
    else:
        scene_label = "Разреженный лёд"
        dominant_label = MASK_CLASSES.get(dominant_class, {}).get("label", "Лёд")

    return {
        "total_pixels": total_pixels,
        "usable_pixels": usable_pixels,
        "vessel_pixels": vessel_pixels,
        "ice_field_pixels": ice_field_pixels,
        "broken_ice_pixels": broken_ice_pixels,
        "slush_ice_pixels": slush_ice_pixels,
        "ice_cover": ice_cover,
        "field_cover": field_cover,
        "broken_cover": broken_cover,
        "slush_cover": slush_cover,
        "field_share": field_share,
        "broken_share": broken_share,
        "slush_share": slush_share,
        "loose_share": loose_share,
        "roughness": roughness,
        "scene_pressure": scene_pressure,
        "dominant_class": MASK_CLASSES.get(dominant_class, {}).get("label", "Открытая вода"),
        "dominant_label": dominant_label,
        "scene_label": scene_label,
        "vessel_present": bool(vessel_pixels),
    }


def scene_level(field_cover: float, scene_pressure: float) -> str:
    if field_cover >= 0.35 or scene_pressure >= 0.45:
        return "danger"
    if field_cover >= 0.16 or scene_pressure >= 0.20:
        return "caution"
    return "ok"


def metric_level(value: float, *, caution_threshold: float, danger_threshold: float) -> str:
    if value >= danger_threshold:
        return "danger"
    if value >= caution_threshold:
        return "caution"
    return "ok"


def derive_display_metrics(state: CamState) -> dict[str, float | int | str | bool | None]:
    raw = state.last_analysis or {
        "ice_cover": 0.0,
        "field_cover": 0.0,
        "broken_cover": 0.0,
        "slush_cover": 0.0,
        "field_share": 0.0,
        "loose_share": 0.0,
        "roughness": 0.0,
        "scene_pressure": 0.0,
        "scene_label": "Открытая вода",
        "dominant_label": "Открытая вода",
        "vessel_present": False,
    }

    field_cover = clamp01(float(raw.get("field_cover", 0.0)))
    ice_cover = clamp01(float(raw.get("ice_cover", 0.0)))
    loose_share = clamp01(float(raw.get("loose_share", 0.0)))
    roughness = clamp01(float(raw.get("roughness", 0.0)))
    scene_pressure = clamp01(float(raw.get("scene_pressure", 0.0)))
    field_share = clamp01(float(raw.get("field_share", 0.0)))

    trend_values = [clamp01(value) for _, value in list(state.trend)[-LIVE_METRIC_WINDOW:]]
    trend_avg = ema_value(trend_values) if trend_values else field_cover

    age_sec = None
    freshness = 1.0
    if state.last_result_at is not None:
        age_sec = max(0.0, time.time() - state.last_result_at)
        freshness = 0.5 ** (age_sec / LIVE_METRIC_FRESHNESS_HALF_LIFE_SEC)

    blended_pressure = clamp01(
        (0.72 * field_cover + 0.18 * scene_pressure + 0.10 * trend_avg) * (0.9 + 0.1 * freshness)
    )

    return {
        "field_cover": field_cover,
        "ice_cover": ice_cover,
        "loose_share": loose_share,
        "scene_pressure": scene_pressure,
        "display_load": blended_pressure,
        "field_share": field_share,
        "freshness": freshness,
        "age_sec": age_sec,
        "trend_points": len(trend_values),
        "scene_label": raw.get("scene_label", "Открытая вода"),
        "dominant_label": raw.get("dominant_label", "Открытая вода"),
        "vessel_present": bool(raw.get("vessel_present", False)),
        "broken_cover": clamp01(float(raw.get("broken_cover", 0.0))),
        "slush_cover": clamp01(float(raw.get("slush_cover", 0.0))),
        "roughness": roughness,
    }


def resize_for_display(frame: np.ndarray | None, max_width: int = DISPLAY_MAX_WIDTH, max_height: int = DISPLAY_MAX_HEIGHT):
    if frame is None:
        return None
    height, width = frame.shape[:2]
    scale = min(max_width / width, max_height / height, 1.0)
    if scale >= 1.0:
        return frame
    new_size = (max(1, int(round(width * scale))), max(1, int(round(height * scale))))
    return cv2.resize(frame, new_size, interpolation=cv2.INTER_AREA)


def level_word(level: str) -> str:
    return {"danger": "bad", "caution": "warn", "ok": "ok"}.get(level, "neutral")


def build_trend_svg(points: list[tuple[float, float]], *, title: str = "Плотность поля по времени", current_label: str = "поле") -> str:
    if not points:
        return (
            "<div class='trend-wrap muted'>"
            "Данные появятся после первого прогноза модели."
            "</div>"
        )

    width = 320
    height = 84
    pad = 4
    values = [max(0.0, min(1.0, v)) for _, v in points]
    n = len(values)

    if n == 1:
        values = values * 2
        n = 2

    step = (width - 2 * pad) / (n - 1)
    pts = []
    for idx, val in enumerate(values):
        x = pad + idx * step
        y = pad + (1.0 - val) * (height - 2 * pad)
        pts.append(f"{x:.1f},{y:.1f}")

    poly_line = " ".join(pts)
    poly_area = f"{pts[0].split(',')[0]},{height - pad} " + poly_line + f" {pts[-1].split(',')[0]},{height - pad}"

    current = values[-1]
    peak = max(values)
    mean = sum(values) / len(values)

    return f"""
    <div class='trend-wrap'>
      <div class='trend-title'>{html_escape(title)}</div>
      <svg class='trend-svg' viewBox='0 0 {width} {height}' preserveAspectRatio='none'>
        <defs>
          <linearGradient id='trendFill' x1='0' x2='0' y1='0' y2='1'>
            <stop offset='0%' stop-color='#5ec8ff' stop-opacity='0.55'/>
            <stop offset='100%' stop-color='#5ec8ff' stop-opacity='0'/>
          </linearGradient>
          <linearGradient id='trendLine' x1='0' x2='1' y1='0' y2='0'>
            <stop offset='0%' stop-color='#7effd9'/>
            <stop offset='100%' stop-color='#5ec8ff'/>
          </linearGradient>
        </defs>
        <polygon points='{poly_area}' fill='url(#trendFill)' stroke='none'/>
        <polyline points='{poly_line}' fill='none' stroke='url(#trendLine)' stroke-width='1.8'
                  stroke-linecap='round' stroke-linejoin='round'/>
    </svg>
    <div class='trend-legend'>
        <span>{html_escape(current_label)} {current * 100:.1f}%</span>
        <span>средн. {mean * 100:.1f}%</span>
        <span>пик {peak * 100:.1f}%</span>
      </div>
    </div>
    """


def render_stat_card(label: str, value: str, hint: str, tone: str = "ok") -> str:
    tone_cls = tone if tone in {"danger", "caution", "ok"} else ""
    return f"""
    <div class='stat-card {tone_cls}'>
      <div class='stat-label'>{html_escape(label)}</div>
      <div class='stat-value'>{html_escape(value)}</div>
      <div class='stat-hint'>{html_escape(hint)}</div>
    </div>
    """


def build_hero(status_by_cam, system_status):
    live_cams = sum(1 for item in status_by_cam.values() if item.get("has_frames"))

    summaries = []
    for cam_id, label in CAMERAS:
        st_obj = st.session_state.cam_states[cam_id]
        metrics = derive_display_metrics(st_obj)
        summaries.append(
            {
                "cam_id": cam_id,
                "label": label,
                "field_cover": float(metrics.get("field_cover", 0.0)),
                "ice_cover": float(metrics.get("ice_cover", 0.0)),
                "loose_share": float(metrics.get("loose_share", 0.0)),
                "scene_pressure": float(metrics.get("scene_pressure", 0.0)),
                "display_load": float(metrics.get("display_load", 0.0)),
                "freshness": float(metrics.get("freshness", 0.0)),
                "scene_label": str(metrics.get("scene_label", "Открытая вода")),
                "dominant_label": str(metrics.get("dominant_label", "Открытая вода")),
                "trend_points": int(metrics.get("trend_points", 0) or 0),
            }
        )

    total_weight = 0.0
    weighted_field = 0.0
    weighted_ice = 0.0
    weighted_loose = 0.0
    weighted_load = 0.0
    for item in summaries:
        weight = 0.35 + 0.65 * item["freshness"]
        total_weight += weight
        weighted_field += item["field_cover"] * weight
        weighted_ice += item["ice_cover"] * weight
        weighted_loose += item["loose_share"] * weight
        weighted_load += item["display_load"] * weight

    if total_weight > 0:
        avg_field = weighted_field / total_weight
        avg_ice = weighted_ice / total_weight
        avg_loose = weighted_loose / total_weight
        avg_load = weighted_load / total_weight
    else:
        avg_field = avg_ice = avg_loose = avg_load = 0.0

    peak_field = max(summaries, key=lambda item: item["field_cover"], default=None)
    worst = max(summaries, key=lambda item: item["display_load"], default=None)
    if worst is None:
        scene_chip = chip("Нет данных", "neutral")
        hero_note = "Ждём первые маски от модели."
    else:
        scene_level_state = scene_level(worst["field_cover"], worst["display_load"])
        scene_chip = chip(worst["scene_label"], level_word(scene_level_state))
        if worst["field_cover"] >= 0.35:
            hero_note = (
                f"Критичнее всего {worst['label']}: сплошной лёд занимает "
                f"{worst['field_cover'] * 100:.0f}% кадра."
            )
        elif worst["field_cover"] < 0.08 and worst["loose_share"] >= 0.65:
            hero_note = (
                f"В основном рыхлый лёд, без заметного сплошного поля. "
                f"Доминирует {worst['dominant_label']}."
            )
        else:
            hero_note = (
                f"Наиболее выраженная зона у {worst['label']}: "
                f"плотность {worst['display_load'] * 100:.0f}%."
            )

    now_local = datetime.now(LOCAL_TZ).strftime("%d.%m.%Y В· %H:%M:%S")
    fresh_cams = sum(1 for item in summaries if item["freshness"] >= 0.5)
    field_cams = sum(1 for item in summaries if item["field_cover"] >= 0.12)

    hero_chips = " ".join(
        [
            chip(f"Москва · {now_local}", "neutral"),
            chip(
                f"Камеры {live_cams}/{len(CAMERAS)}",
                "ok" if live_cams == len(CAMERAS) else "warn",
            ),
            chip(
                f"Свежее {fresh_cams}/{len(CAMERAS)}",
                "ok" if fresh_cams == len(CAMERAS) else "warn",
            ),
            chip(
                f"Поле {field_cams}/{len(CAMERAS)}",
                "danger" if field_cams == len(CAMERAS) and field_cams > 0 else "warn" if field_cams else "ok",
            ),
            scene_chip,
        ]
    )

    stat_cards = "".join(
        [
            render_stat_card(
                "Сплошное поле",
                f"{(peak_field['field_cover'] if peak_field else 0.0) * 100:.0f}%",
                f"Среднее {avg_field * 100:.0f}% · {peak_field['label'] if peak_field else 'нет данных'}",
                "danger" if (peak_field["field_cover"] if peak_field else 0.0) >= 0.35 else "caution" if (peak_field["field_cover"] if peak_field else 0.0) >= 0.16 else "ok",
            ),
            render_stat_card(
                "Лёд в кадре",
                f"{avg_ice * 100:.0f}%",
                "Общая доля льда без учёта судна",
                "caution" if avg_ice >= 0.45 else "ok",
            ),
            render_stat_card(
                "Рыхлый лёд",
                f"{avg_loose * 100:.0f}%",
                "Битый лёд и шуга внутри ледовой зоны",
                "ok",
            ),
            render_stat_card(
                "Плотность поля",
                f"{(worst['display_load'] if worst else 0.0) * 100:.0f}%",
                f"Среднее {avg_load * 100:.0f}% · {worst['label'] if worst else 'нет данных'}",
                "danger" if (worst['display_load'] if worst else 0.0) >= 0.35 else "caution" if (worst['display_load'] if worst else 0.0) >= 0.16 else "ok",
            ),
        ]
    )

    hero_note_text = hero_note.rstrip(".")

    return f"""
    <div class='hero'>
      <div class='hero-main'>
        <div class='hero-title'>
          <span class='hero-logo'>◉</span>
          <span>Ледовый монитор</span>
        </div>
        <div class='hero-meta'>{hero_chips}</div>
        <div class='hero-sub'>{html_escape(hero_note_text)}</div>
      </div>
      <div class='hero-stats'>{stat_cards}</div>
    </div>
    """.replace(
        "\n", ""
    )


def build_camera_card(cam_id: str, label: str, state: CamState, cam_status: dict):
    metrics = derive_display_metrics(state)
    field_cover = clamp01(float(metrics.get("field_cover", 0.0)))
    ice_cover = clamp01(float(metrics.get("ice_cover", 0.0)))
    loose_share = clamp01(float(metrics.get("loose_share", 0.0)))
    scene_pressure = clamp01(float(metrics.get("scene_pressure", 0.0)))
    display_load = clamp01(float(metrics.get("display_load", scene_pressure)))
    scene_name = str(metrics.get("scene_label", "Открытая вода"))
    dominant_label = str(metrics.get("dominant_label", "Открытая вода"))
    level = scene_level(field_cover, display_load)
    level_word_ru = {"danger": "Опасно", "caution": "Внимание", "ok": "Норма"}[level]

    lag_text = "нет"
    if state.last_frame_ts is not None and state.last_result_source_ts is not None:
        lag_text = f"{max(0.0, state.last_frame_ts - state.last_result_source_ts):.1f} с"

    cam_dot = {
        "ok": "",
        "caution": "warn",
        "danger": "bad",
    }[level]

    head = f"""
    <div class='cam-head'>
      <div class='cam-title'><span class='cam-dot {cam_dot}'></span>{html_escape(label)}</div>
      <div class='chip-row'>{chip(level_word_ru, level_word(level))}</div>
    </div>
    """

    trend_points = int(metrics.get("trend_points", 0) or 0)
    if trend_points:
        freshness_text = f"{metrics.get('freshness', 1.0) * 100:.0f}%"
        if loose_share >= 0.65 and field_cover < 0.08:
            metric_note = (
                f"{scene_name}. В основном {dominant_label}, сплошного поля почти нет. "
                f"Сглаживание по последним {trend_points} результатам, свежесть сигнала {freshness_text}."
            )
        else:
            metric_note = (
                f"{scene_name}. Доминирует {dominant_label}. "
                f"Сглаживание по последним {trend_points} результатам, свежесть сигнала {freshness_text}."
            )
    else:
        metric_note = f"{scene_name}. Ожидание первых результатов модели."

    field_level = metric_level(field_cover, caution_threshold=0.16, danger_threshold=0.35)
    ice_level = metric_level(ice_cover, caution_threshold=0.30, danger_threshold=0.60)
    loose_level = metric_level(loose_share, caution_threshold=0.35, danger_threshold=0.65)
    field_value_cls = {"danger": "bad", "caution": "warn", "ok": "ok"}[field_level]
    field_bar_cls = {"danger": "danger", "caution": "caution", "ok": ""}[field_level]
    ice_value_cls = {"danger": "bad", "caution": "warn", "ok": "ok"}[ice_level]
    ice_bar_cls = {"danger": "danger", "caution": "caution", "ok": ""}[ice_level]
    loose_value_cls = {"danger": "bad", "caution": "warn", "ok": "ok"}[loose_level]
    loose_bar_cls = {"danger": "danger", "caution": "caution", "ok": ""}[loose_level]

    metrics_block = f"""
    <div class='cam-metrics'>
      <div class='metric'>
        <div class='metric-label'>Сплошное поле</div>
        <div class='metric-value {field_value_cls}'>{field_cover * 100:.1f}%</div>
        <div class='metric-bar {field_bar_cls}'><span style='width:{field_cover * 100:.1f}%'></span></div>
      </div>
      <div class='metric'>
        <div class='metric-label'>Лёд всего</div>
        <div class='metric-value {ice_value_cls}'>{ice_cover * 100:.1f}%</div>
        <div class='metric-bar {ice_bar_cls}'><span style='width:{ice_cover * 100:.1f}%'></span></div>
      </div>
      <div class='metric'>
        <div class='metric-label'>Рыхлый лёд</div>
        <div class='metric-value {loose_value_cls}'>{loose_share * 100:.1f}%</div>
        <div class='metric-bar {loose_bar_cls}'><span style='width:{loose_share * 100:.1f}%'></span></div>
      </div>
    </div>
    <div class='metric-note'>
       {metric_note}
    </div>
    {build_trend_svg(list(state.trend), title="Сплошное поле по времени", current_label="поле")}
    """

    foot = f"""
    <div class='cam-foot'>
      <span>Кадр · {format_ts(state.last_frame_ts)}</span>
      <span>Маска · {format_ts(state.last_result_ts)}</span>
    </div>
    """

    return (
        f"<div class='cam-card {level if level != 'ok' else ''}'>"
        f"{head}"
        f"__IMAGE__"
        f"{metrics_block}"
        f"{foot}"
        f"</div>"
    )


def render_services_panel(slot, system_status):
    signature = tuple(
        (
            item.get("name"),
            item.get("ok"),
            item.get("host"),
            item.get("port"),
        )
        for item in system_status.get("services", [])
    )
    if st.session_state.get("services_signature") == signature:
        return
    st.session_state.services_signature = signature
    with slot.container():
        service_index = {item["name"]: item for item in system_status.get("services", [])}
        rows = []
        for name in CORE_SERVICE_ORDER:
            item = service_index.get(name)
            if item is None:
                continue
            ok = bool(item.get("ok"))
            rows.append(
                f"""
                <div class='service-item'>
                  <div>
                    <div class='service-name'>{html_escape(SERVICE_LABELS.get(name, name))}</div>
                    <div class='service-host'>{html_escape(item['host'])}:{item['port']}</div>
                  </div>
                  <span></span>
                  <span class='service-pill {"ok" if ok else "bad"}'>{"Работает" if ok else "Сбой"}</span>
                </div>
                """
        )

        body = "".join(rows) if rows else "<div class='muted'>Проверка сервисов еще выполняется.</div>"
        slot.markdown(
            dedent(
                f"""
                <div class='panel-card'>
                  <div class='panel-title'>Сервисы</div>
                  <div class='service-list'>{body}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


def render_legend_panel(slot):
    if st.session_state.get("legend_rendered"):
        return
    with slot.container():
        rows = []
        for cls_id, meta in MASK_CLASSES.items():
            rows.append(
                dedent(
                    f"""
                    <div class='legend-item'>
                      <span class='legend-swatch' style='background:{meta["hex"]};'></span>
                      <div>
                        <div class='legend-label'>{html_escape(MASK_HUD_LABELS.get(cls_id, f"MASK {cls_id}"))}</div>
                        <div class='legend-sub'>{html_escape(meta["label"])}</div>
                      </div>
                      <span class='legend-hex'>{html_escape(meta["hex"])}</span>
                    </div>
                    """
                ).strip()
            )
        slot.markdown(
            dedent(
                f"""
                <div class='panel-card'>
                  <div class='panel-title'>Легенда масок</div>
                  <div class='legend-grid'>{"".join(rows)}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )
        st.session_state.legend_rendered = True


def render_logs_panel(slot, entries):
    signature = tuple(
        (entry.get("ts"), entry.get("level"), entry.get("source"), entry.get("cam_id"), entry.get("message"))
        for entry in entries[:MAX_LOG_ROWS]
    )
    if st.session_state.get("logs_signature") == signature:
        return
    st.session_state.logs_signature = signature
    with slot.container():
        if not entries:
            body = "<div class='muted'>События контура появятся после прогрева сервисов.</div>"
        else:
            rows = []
            for entry in entries[:MAX_LOG_ROWS]:
                ts = parse_ts(entry.get("ts"))
                ts_text = ts.astimezone(LOCAL_TZ).strftime("%H:%M:%S") if ts else "—"
                level = entry.get("level", "info")
                source = entry.get("source", "system")
                cam_id = entry.get("cam_id")
                source_text = source if not cam_id else f"{source}:{cam_id}"
                rows.append(
                    dedent(
                        f"""
                        <div class='log-entry'>
                          <div class='log-time'>{html_escape(ts_text)}</div>
                          <div>
                            <div class='log-meta'>
                              <span class='log-tag {level}'>{html_escape(LOG_LEVEL_LABEL.get(level, level.upper()))}</span>
                              <span class='log-source'>{html_escape(source_text)}</span>
                            </div>
                            <div class='log-message'>{html_escape(entry.get("message", ""))}</div>
                          </div>
                          <span></span>
                        </div>
                        """
                    ).strip()
                )
            body = "".join(rows)

        slot.markdown(
            dedent(
                f"""
                <div class='panel-card'>
                  <div class='panel-title'>Журнал</div>
                  <div class='log-list'>{body}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Р—Р°РїСѓСЃРє WS-РІРѕСЂРєРµСЂРѕРІ
# ---------------------------------------------------------------------------
if not st.session_state.ws_started:
    for cam_id, _ in CAMERAS:
        state = st.session_state.cam_states[cam_id]
        Thread(target=ws_worker, args=(FRAME_WS_URL, cam_id, state.frame_queue), daemon=True).start()
        Thread(target=ws_worker, args=(RESULT_WS_URL, cam_id, state.result_queue), daemon=True).start()
    st.session_state.ws_started = True


# ---------------------------------------------------------------------------
# Р Р°Р·РјРµС‚РєР°
# ---------------------------------------------------------------------------
header_slot = st.empty()
banner_slot = st.empty()


cam_a_col, cam_b_col = st.columns([1.0, 1.0], gap="small")

cam_slots = {}
for (cam_id, _label), column in zip(CAMERAS, [cam_a_col, cam_b_col]):
    with column:
        cam_slots[cam_id] = {
            "card_header": st.empty(),
            "image": st.empty(),
            "card_footer": st.empty(),
        }

legend_col, services_col, logs_col = st.columns([0.9, 1.05, 1.25], gap="small")

with legend_col:
    legend_slot = st.empty()
with services_col:
    services_slot = st.empty()
with logs_col:
    logs_slot = st.empty()


status_by_cam = {}
system_status = {}
log_entries = []

last_status_refresh = 0.0
last_system_refresh = 0.0
last_log_refresh = 0.0


def render_cam(cam_id: str, label: str):
    state = st.session_state.cam_states[cam_id]
    with state.lock:
        frame = state.last_frame
        mask = state.last_mask
        cam_status = dict(status_by_cam.get(cam_id, {}))
        frame_ts = state.last_frame_ts
        result_ts = state.last_result_ts
        result_source_ts = state.last_result_source_ts

    preview_frame = frame
    selected_frame_ts = frame_ts
    mask_is_fresh = state.last_result_at is not None and (time.time() - state.last_result_at) <= MAX_MASK_AGE_SEC
    render_key = (
        selected_frame_ts,
        preview_frame.shape[:2] if preview_frame is not None else None,
        mask.shape[:2] if mask is not None else None,
    )
    preview = state.last_render_preview
    render_changed = preview_frame is not None and render_key != state.last_render_key
    if render_changed:
        display_frame = resize_for_display(preview_frame)
        preview = build_overlay(display_frame, mask, cam_id) if mask_is_fresh else display_frame
        state.last_render_key = render_key
        state.last_render_preview = preview

    card_html = build_camera_card(cam_id, label, state, cam_status)
    card_changed = card_html != state.last_card_html
    if card_changed:
        header_html, footer_html = card_html.split("__IMAGE__")
        cam_slots[cam_id]["card_header"].markdown(header_html, unsafe_allow_html=True)
        cam_slots[cam_id]["card_footer"].markdown(footer_html, unsafe_allow_html=True)
        state.last_card_html = card_html

    if render_changed and preview is not None:
        cam_slots[cam_id]["image"].image(preview, channels="BGR", use_column_width=True)
    elif state.last_render_preview is None:
        cam_slots[cam_id]["image"].info("Ожидание видеопотока")


# ---------------------------------------------------------------------------
# Р“Р»Р°РІРЅС‹Р№ С†РёРєР»
# ---------------------------------------------------------------------------
while True:
    update_state_from_streams()
    now = time.time()
    startup_grace_sec = 30.0
    ready_to_warn = (now - st.session_state.started_at) >= startup_grace_sec

    if now - last_status_refresh >= STATUS_REFRESH_SEC:
        status_by_cam, status_error = load_pipeline_status()
        if status_error and ready_to_warn:
            banner_slot.warning(status_error)
        elif status_by_cam and not any(item.get("has_results") for item in status_by_cam.values()) and ready_to_warn:
            banner_slot.warning(
                "Видео поступает, но модель ещё не прислала маску. Подождите прогрев."
            )
        else:
            banner_slot.empty()
        last_status_refresh = now

    if now - last_system_refresh >= SYSTEM_REFRESH_SEC:
        system_status, system_error = load_system_status()
        if system_error and ready_to_warn:
            banner_slot.warning(system_error)
        last_system_refresh = now

    if now - last_log_refresh >= LOG_REFRESH_SEC:
        log_entries, _ = load_logs()
        last_log_refresh = now

    hero_second = int(now)
    hero_html = build_hero(status_by_cam, system_status)
    if st.session_state.hero_second != hero_second or st.session_state.get("hero_html") != hero_html:
        header_slot.markdown(hero_html, unsafe_allow_html=True)
        st.session_state.hero_second = hero_second
        st.session_state.hero_html = hero_html

    for cam_id, label in CAMERAS:
        render_cam(cam_id, label)

    render_legend_panel(legend_slot)
    render_services_panel(services_slot, system_status)
    render_logs_panel(logs_slot, log_entries)

    time.sleep(RENDER_INTERVAL_SEC)
