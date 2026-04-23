from __future__ import annotations

import argparse
import base64
import json
import socket
import statistics
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import cv2
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


DEFAULT_CAMERA_COUNTS = [1, 2]
DEFAULT_STREAM_PREFIX = "bench_cam"
DEFAULT_FRAME_SIZE = 320
DEFAULT_FPS = 1.0
DEFAULT_WARMUP_SEC = 8.0
DEFAULT_MEASURE_SEC = 20.0
DEFAULT_DRAIN_SEC = 4.0
DEFAULT_MAXLEN = 100


class RedisClient:
    def __init__(self, host: str, port: int, *, timeout: float = 60.0):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._lock = threading.Lock()

    @staticmethod
    def _encode(value: object) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")

    @staticmethod
    def _encode_command(parts: list[object]) -> bytes:
        payload = [f"*{len(parts)}\r\n".encode("ascii")]
        for part in parts:
            data = RedisClient._encode(part)
            payload.append(f"${len(data)}\r\n".encode("ascii"))
            payload.append(data)
            payload.append(b"\r\n")
        return b"".join(payload)

    def _read_line(self) -> bytes:
        line = b""
        while not line.endswith(b"\r\n"):
            chunk = self._sock.recv(1)
            if not chunk:
                raise ConnectionError("Redis connection closed")
            line += chunk
        return line[:-2]

    def _read_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Redis connection closed")
            buf.extend(chunk)
        return bytes(buf)

    def _read_response(self):
        prefix = self._read_exact(1)
        if prefix == b"+":
            return self._read_line().decode("utf-8")
        if prefix == b"-":
            raise RuntimeError(self._read_line().decode("utf-8"))
        if prefix == b":":
            return int(self._read_line())
        if prefix == b"$":
            length = int(self._read_line())
            if length == -1:
                return None
            data = self._read_exact(length)
            self._read_exact(2)
            return data.decode("utf-8")
        if prefix == b"*":
            length = int(self._read_line())
            if length == -1:
                return None
            return [self._read_response() for _ in range(length)]
        raise RuntimeError(f"Unexpected Redis response prefix: {prefix!r}")

    def execute(self, *parts: object):
        payload = self._encode_command(list(parts))
        with self._lock:
            self._sock.sendall(payload)
            return self._read_response()

    def ping(self):
        return self.execute("PING")

    def delete(self, *keys: str):
        if not keys:
            return 0
        return self.execute("DEL", *keys)

    def xadd(self, stream: str, payload: dict[str, object], maxlen: int | None = None):
        parts: list[object] = ["XADD", stream]
        if maxlen is not None:
            parts.extend(["MAXLEN", "~", str(maxlen)])
        parts.append("*")
        for key, value in payload.items():
            parts.append(key)
            parts.append(value)
        return self.execute(*parts)

    def xread(self, stream_name: str, last_id: str, *, count: int = 20, block: int = 500):
        response = self.execute("XREAD", "COUNT", str(count), "BLOCK", str(block), "STREAMS", stream_name, last_id)
        if response is None:
            return []
        return response

    def close(self):
        try:
            self._sock.close()
        except Exception:
            pass


@dataclass
class ScenarioState:
    measure_started_at: float | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    records: list[dict] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    published_measured: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    processed_measured: dict[str, int] = field(default_factory=lambda: defaultdict(int))


def parse_int_list(value: str) -> list[int]:
    return [int(item) for item in value.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure hot-path inference latency as camera count grows."
    )
    parser.add_argument(
        "--camera-counts",
        default="1,2,4",
        help="Comma-separated camera counts to test.",
    )
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument(
        "--stream-prefix",
        default=DEFAULT_STREAM_PREFIX,
        help="Camera id prefix. Streams become frames:<prefix>_<n>.",
    )
    parser.add_argument("--fps", type=float, default=DEFAULT_FPS)
    parser.add_argument("--warmup-sec", type=float, default=DEFAULT_WARMUP_SEC)
    parser.add_argument("--measure-sec", type=float, default=DEFAULT_MEASURE_SEC)
    parser.add_argument("--drain-sec", type=float, default=DEFAULT_DRAIN_SEC)
    parser.add_argument("--frame-size", type=int, default=DEFAULT_FRAME_SIZE)
    parser.add_argument("--maxlen", type=int, default=DEFAULT_MAXLEN)
    parser.add_argument(
        "--out-dir",
        default="benchmarks/hotpath",
        help="Directory for CSV and PNG outputs.",
    )
    return parser.parse_args()


def make_camera_ids(count: int, prefix: str) -> list[str]:
    return [f"{prefix}_{idx}" for idx in range(1, count + 1)]


def make_frame(camera_index: int, seq: int, size: int) -> np.ndarray:
    rng = np.random.default_rng(camera_index * 1_000_000 + seq)
    frame = np.zeros((size, size, 3), dtype=np.uint8)

    horizon = int(size * 0.34)
    frame[:horizon, :] = (205, 180, 130)
    frame[horizon:, :] = (60, 90, 135)

    for band in range(0, size, max(8, size // 32)):
        color = int(80 + 25 * np.sin((seq + band) * 0.08 + camera_index))
        frame[horizon + band // 4 : min(size, horizon + band // 4 + 2), :] = (
            color,
            color + 12,
            color + 18,
        )

    patch_count = 14
    for _ in range(patch_count):
        x = int(rng.integers(0, size))
        y = int(rng.integers(horizon, size))
        radius = int(rng.integers(max(8, size // 40), max(12, size // 18)))
        cv2.circle(frame, (x, y), radius, (230, 230, 230), -1)

    ship_w = int(size * 0.30)
    ship_h = int(size * 0.10)
    x_shift = int(np.sin(seq * 0.15 + camera_index) * size * 0.015)
    x1 = max(0, size // 2 - ship_w // 2 + x_shift)
    y1 = int(size * 0.58)
    cv2.rectangle(frame, (x1, y1), (min(size - 1, x1 + ship_w), min(size - 1, y1 + ship_h)), (45, 45, 55), -1)

    noise = rng.normal(0, 6, frame.shape).astype(np.int16)
    noisy = np.clip(frame.astype(np.int16) + noise, 0, 255).astype(np.uint8)
    return noisy


def encode_frame(frame: np.ndarray) -> str:
    _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return base64.b64encode(buf.tobytes()).decode("ascii")


def clear_streams(redis_client: RedisClient, camera_ids: list[str]) -> None:
    keys = []
    for cam_id in camera_ids:
        keys.append(f"frames:{cam_id}")
        keys.append(f"results:{cam_id}")
    if keys:
        redis_client.delete(*keys)


def publisher_loop(
    redis_client: RedisClient,
    cam_id: str,
    camera_index: int,
    frame_size: int,
    fps: float,
    maxlen: int,
    state: ScenarioState,
    start_at: float,
    stop_at: float,
) -> None:
    stream_name = f"frames:{cam_id}"
    interval = 1.0 / fps
    seq = 0
    next_tick = start_at

    while not state.stop_event.is_set():
        now = time.perf_counter()
        if now >= stop_at:
            break
        if now < next_tick:
            time.sleep(min(0.02, next_tick - now))
            continue

        frame = make_frame(camera_index, seq, frame_size)
        source_ts = time.time()
        payload = {
            "cam_id": cam_id,
            "ts": f"{source_ts:.6f}",
            "frame_seq": str(seq),
            "frame": encode_frame(frame),
        }
        try:
            redis_client.xadd(stream_name, payload, maxlen=maxlen)
        except (TimeoutError, OSError, RuntimeError) as exc:
            print(f"[{cam_id}] xadd error: {exc}", flush=True)
            time.sleep(0.5)
            continue

        if state.measure_started_at is not None and source_ts >= state.measure_started_at:
            with state.lock:
                state.published_measured[cam_id] += 1

        seq += 1
        next_tick += interval


def collector_loop(
    redis_client: RedisClient,
    cam_id: str,
    state: ScenarioState,
    measure_started_event: threading.Event,
    stop_at: float,
) -> None:
    stream_name = f"results:{cam_id}"
    last_id = "0-0"

    while True:
        if state.stop_event.is_set() and time.perf_counter() >= stop_at:
            break

        try:
            msgs = redis_client.xread(stream_name, last_id, count=20, block=500)
        except (TimeoutError, OSError, RuntimeError) as exc:
            print(f"[{cam_id}] xread error: {exc}", flush=True)
            time.sleep(0.5)
            continue
        if not msgs:
            continue

        for _, entries in msgs:
            for msg_id, data_list in entries:
                last_id = msg_id
                if not measure_started_event.is_set():
                    continue

                data = {
                    data_list[idx]: data_list[idx + 1]
                    for idx in range(0, len(data_list), 2)
                }

                raw_source_ts = data.get("source_ts") or data.get("ts")
                if not raw_source_ts:
                    continue

                source_ts = float(raw_source_ts)
                measure_started_at = state.measure_started_at
                if measure_started_at is None or source_ts < measure_started_at:
                    continue

                received_ts = time.time()
                processed_ts_raw = data.get("processed_ts")
                inference_ms_raw = data.get("inference_ms")
                queue_age_ms_raw = data.get("queue_age_ms")
                frame_seq_raw = data.get("frame_seq")

                processed_ts = float(processed_ts_raw) if processed_ts_raw else received_ts
                inference_ms = float(inference_ms_raw) if inference_ms_raw else None
                queue_age_ms = float(queue_age_ms_raw) if queue_age_ms_raw else (processed_ts - source_ts) * 1000.0
                frame_seq = int(frame_seq_raw) if frame_seq_raw not in (None, "") else None

                record = {
                    "cam_id": cam_id,
                    "frame_seq": frame_seq,
                    "source_ts": source_ts,
                    "processed_ts": processed_ts,
                    "received_ts": received_ts,
                    "worker_processing_ms": queue_age_ms,
                    "collector_delivery_ms": (received_ts - source_ts) * 1000.0,
                    "inference_ms": inference_ms,
                }

                with state.lock:
                    state.records.append(record)
                    state.processed_measured[cam_id] += 1


def summarize_records(
    records: pd.DataFrame,
    camera_count: int,
    duration_sec: float,
    published_total: int,
) -> dict:
    end_to_end_ms = records["worker_processing_ms"].astype(float)
    delivery_ms = records["collector_delivery_ms"].astype(float)
    inference_ms = records["inference_ms"].dropna().astype(float)

    processed_total = int(len(records))

    return {
        "camera_count": camera_count,
        "published_measured": published_total,
        "processed_measured": processed_total,
        "drop_rate": 1.0 - (processed_total / published_total) if published_total else 0.0,
        "end_to_end_median_ms": float(end_to_end_ms.median()) if not end_to_end_ms.empty else 0.0,
        "end_to_end_p95_ms": float(end_to_end_ms.quantile(0.95)) if not end_to_end_ms.empty else 0.0,
        "delivery_median_ms": float(delivery_ms.median()) if not delivery_ms.empty else 0.0,
        "delivery_p95_ms": float(delivery_ms.quantile(0.95)) if not delivery_ms.empty else 0.0,
        "inference_median_ms": float(inference_ms.median()) if not inference_ms.empty else 0.0,
        "inference_p95_ms": float(inference_ms.quantile(0.95)) if not inference_ms.empty else 0.0,
        "throughput_fps": processed_total / duration_sec if duration_sec > 0 else 0.0,
    }


def plot_results(summary_df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(
        summary_df["camera_count"],
        summary_df["end_to_end_median_ms"],
        marker="o",
        linewidth=2,
        label="median end-to-end latency",
    )
    ax.plot(
        summary_df["camera_count"],
        summary_df["end_to_end_p95_ms"],
        marker="o",
        linewidth=2,
        label="p95 end-to-end latency",
    )
    ax.set_xlabel("Camera count")
    ax.set_ylabel("Latency, ms")
    ax.set_title("Inference latency versus camera count")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    fig.savefig(out_dir / "hotpath_latency_vs_cameras.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(
        summary_df["camera_count"],
        summary_df["throughput_fps"],
        marker="o",
        linewidth=2,
        label="processed FPS",
    )
    ax.plot(
        summary_df["camera_count"],
        summary_df["drop_rate"],
        marker="o",
        linewidth=2,
        label="drop rate",
    )
    ax.set_xlabel("Camera count")
    ax.set_ylabel("Rate")
    ax.set_title("Throughput and drop rate versus camera count")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    fig.savefig(out_dir / "hotpath_throughput_vs_cameras.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def run_scenario(
    redis_client: RedisClient,
    camera_count: int,
    args: argparse.Namespace,
    out_dir: Path,
) -> tuple[pd.DataFrame, dict]:
    camera_ids = make_camera_ids(camera_count, args.stream_prefix)
    clear_streams(redis_client, camera_ids)

    state = ScenarioState()
    measure_started_event = threading.Event()
    start_time = time.perf_counter()
    warmup_end = start_time + args.warmup_sec
    stop_at = warmup_end + args.measure_sec
    state.measure_started_at = None

    pub_threads = []
    col_threads = []

    for idx, cam_id in enumerate(camera_ids, start=1):
        pub_client = RedisClient(host=args.redis_host, port=args.redis_port)
        col_client = RedisClient(host=args.redis_host, port=args.redis_port)

        pub_thread = threading.Thread(
            target=publisher_loop,
            args=(pub_client, cam_id, idx, args.frame_size, args.fps, args.maxlen, state, start_time, stop_at),
            daemon=True,
        )
        col_thread = threading.Thread(
            target=collector_loop,
            args=(col_client, cam_id, state, measure_started_event, stop_at),
            daemon=True,
        )
        pub_threads.append(pub_thread)
        col_threads.append(col_thread)

    for thread in col_threads + pub_threads:
        thread.start()

    time.sleep(args.warmup_sec)
    state.measure_started_at = time.time()
    measure_started_event.set()
    print(f"[{camera_count} cams] Measurement started")

    time.sleep(args.measure_sec + args.drain_sec)
    state.stop_event.set()

    for thread in pub_threads + col_threads:
        thread.join(timeout=10)

    with state.lock:
        records = pd.DataFrame(state.records)

    if records.empty:
        summary = {
            "camera_count": camera_count,
            "published_measured": 0,
            "processed_measured": 0,
            "drop_rate": 0.0,
            "end_to_end_median_ms": 0.0,
            "end_to_end_p95_ms": 0.0,
            "delivery_median_ms": 0.0,
            "delivery_p95_ms": 0.0,
            "inference_median_ms": 0.0,
            "inference_p95_ms": 0.0,
            "throughput_fps": 0.0,
        }
        return records, summary

    published_total = int(sum(state.published_measured.values()))
    summary = summarize_records(records, camera_count, args.measure_sec, published_total)
    return records, summary


def main() -> None:
    args = parse_args()
    camera_counts = parse_int_list(args.camera_counts)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    redis_client = RedisClient(host=args.redis_host, port=args.redis_port)
    redis_client.ping()

    all_records = []
    summaries = []

    for camera_count in camera_counts:
        print(f"Running scenario for {camera_count} camera(s)...")
        records, summary = run_scenario(redis_client, camera_count, args, out_dir)
        if not records.empty:
            records = records.copy()
            records["camera_count"] = camera_count
            all_records.append(records)
        summaries.append(summary)
        print(
            f"  processed={summary['processed_measured']} "
            f"published={summary['published_measured']} "
            f"median={summary['end_to_end_median_ms']:.1f} ms "
            f"p95={summary['end_to_end_p95_ms']:.1f} ms"
        )

    summary_df = pd.DataFrame(summaries).sort_values("camera_count").reset_index(drop=True)
    trial_df = pd.concat(all_records, ignore_index=True) if all_records else pd.DataFrame()

    summary_path = out_dir / "hotpath_summary.csv"
    trials_path = out_dir / "hotpath_trials.csv"
    summary_df.to_csv(summary_path, index=False)
    trial_df.to_csv(trials_path, index=False)
    plot_results(summary_df, out_dir)

    print("\nSummary:")
    print(summary_df.to_string(index=False))
    print(f"\nSaved summary to {summary_path}")
    print(f"Saved trials to  {trials_path}")
    print(f"Saved figures to  {out_dir}")


if __name__ == "__main__":
    main()
