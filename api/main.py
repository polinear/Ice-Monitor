"""
FastAPI service for live frame streaming, live inference results, and TimescaleDB
history.
"""
import os
import asyncio
import json
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_HOST = os.environ.get("REDIS_HOST", "broker")
CAMERAS = ("cam_a", "cam_b")
SYSTEM_METRICS_INTERVAL_SECONDS = 30
SERVICE_PORTS = {
    "api": ("api", 8000),
    "frontend": ("frontend", 8501),
    "redis": ("broker", 6379),
    "timescaledb": ("db", 5432),
    "zookeeper": ("zookeeper", 2181),
    "kafka": ("kafka", 9092),
    "namenode": ("namenode", 9870),
    "datanode": ("datanode", 9864),
    "resourcemanager": ("resourcemanager", 8088),
    "spark_master": ("spark-master", 7077),
    "spark_worker_1": ("spark-worker-1", 8081),
    "spark_worker_2": ("spark-worker-2", 8081),
    "hive_metastore": ("hive-metastore", 9083),
    "hive_server": ("hive-server", 10000),
    "jupyter": ("jupyter", 8888),
    "superset": ("superset", 8088),
}


class LogBuffer:
    def __init__(self, max_entries: int = 300):
        self.entries: deque[dict] = deque(maxlen=max_entries)

    def add(
        self,
        source: str,
        message: str,
        *,
        level: str = "info",
        cam_id: str | None = None,
        details: dict | None = None,
    ):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "source": source,
            "message": message,
            "cam_id": cam_id,
            "details": details or {},
        }
        self.entries.appendleft(entry)
        return entry

    def list(self, limit: int = 100):
        return list(self.entries)[:limit]


log_buffer = LogBuffer()


def log_event(
    source: str,
    message: str,
    *,
    level: str = "info",
    cam_id: str | None = None,
    details: dict | None = None,
):
    log_buffer.add(source, message, level=level, cam_id=cam_id, details=details)
    prefix = f"[{level.upper()}][{source}]"
    if cam_id:
        prefix += f"[{cam_id}]"
    print(f"{prefix} {message}", flush=True)


class ChannelManager:
    def __init__(self):
        self.clients: dict[str, list[WebSocket]] = defaultdict(list)
        self.last_message: dict[str, str] = {}

    async def connect(self, ws: WebSocket, channel_key: str):
        await ws.accept()
        self.clients[channel_key].append(ws)

        latest = self.last_message.get(channel_key)
        if latest is not None:
            try:
                await ws.send_text(latest)
            except Exception:
                self.disconnect(ws, channel_key)

    def disconnect(self, ws: WebSocket, channel_key: str):
        if ws in self.clients[channel_key]:
            self.clients[channel_key].remove(ws)

    async def broadcast(self, channel_key: str, message: str):
        self.last_message[channel_key] = message
        dead = []
        for ws in self.clients[channel_key]:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, channel_key)


frame_manager = ChannelManager()
result_manager = ChannelManager()


async def frame_reader(cam_id: str, redis_client: aioredis.Redis):
    stream_name = f"frames:{cam_id}"
    last_id = "$"
    seen_first_frame = False

    while True:
        try:
            msgs = await redis_client.xread({stream_name: last_id}, count=1, block=1000)
            if not msgs:
                await asyncio.sleep(0)
                continue

            _, entries = msgs[0]
            for msg_id, data in entries:
                last_id = msg_id
                if not seen_first_frame:
                    log_event("ingest", "First live frame reached API", cam_id=cam_id)
                    seen_first_frame = True
                payload = json.dumps(
                    {
                        "ts": data["ts"],
                        "cam_id": data["cam_id"],
                        "frame": data["frame"],
                    }
                )
                await frame_manager.broadcast(cam_id, payload)
        except Exception as e:
            log_event("frame_reader", f"Loop error: {e}", level="error", cam_id=cam_id)
            await asyncio.sleep(2)


async def result_reader(cam_id: str, redis_client: aioredis.Redis, db_pool: asyncpg.Pool):
    stream_name = f"results:{cam_id}"
    last_id = "$"
    seen_first_result = False
    seen_first_db_write = False

    while True:
        try:
            msgs = await redis_client.xread({stream_name: last_id}, count=1, block=1000)
            if not msgs:
                await asyncio.sleep(0)
                continue

            _, entries = msgs[0]
            for msg_id, data in entries:
                last_id = msg_id
                if not seen_first_result:
                    log_event("inference", "First model result reached API", cam_id=cam_id)
                    seen_first_result = True

                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO ice_metrics
                            (ts, cam_id, ice_conc, ice_severity, vessel_present, pixels_json)
                            VALUES (to_timestamp($1), $2, $3, $4, $5, $6)
                            """,
                            float(data["ts"]),
                            cam_id,
                            float(data["ice_conc"]),
                            float(data["ice_severity"]),
                            data["vessel_present"] == "1",
                            data["pixels_json"],
                        )
                    if not seen_first_db_write:
                        log_event("database", "First metrics row saved to TimescaleDB", cam_id=cam_id)
                        seen_first_db_write = True
                except Exception as e:
                    log_event("database", f"Write error: {e}", level="error", cam_id=cam_id)

                payload = json.dumps(
                    {
                        "ts": data["ts"],
                        "cam_id": data["cam_id"],
                        "source_ts": data.get("source_ts"),
                        "frame_seq": data.get("frame_seq"),
                        "processed_ts": data.get("processed_ts"),
                        "queue_age_ms": data.get("queue_age_ms"),
                        "inference_ms": data.get("inference_ms"),
                        "mask": data["mask"],
                        "ice_conc": float(data["ice_conc"]),
                        "ice_severity": float(data["ice_severity"]),
                        "vessel_present": data["vessel_present"] == "1",
                        "pixels": json.loads(data["pixels_json"]),
                        "class_names": json.loads(data["class_names"]),
                    }
                )
                await result_manager.broadcast(cam_id, payload)
        except Exception as e:
            log_event("result_reader", f"Loop error: {e}", level="error", cam_id=cam_id)
            await asyncio.sleep(2)


async def get_stream_status(redis_client: aioredis.Redis, stream_name: str):
    try:
        length = await redis_client.xlen(stream_name)
        latest_entries = await redis_client.xrevrange(stream_name, count=1)
        last_ts = None
        if latest_entries:
            _, data = latest_entries[0]
            if "ts" in data:
                last_ts = float(data["ts"])
        return {"stream": stream_name, "length": length, "last_ts": last_ts}
    except Exception as e:
        return {
            "stream": stream_name,
            "length": None,
            "last_ts": None,
            "error": str(e),
        }


async def get_db_rows_by_cam(db_pool: asyncpg.Pool):
    rows = await db_pool.fetch(
        """
        SELECT cam_id, COUNT(*)::BIGINT AS db_rows
        FROM ice_metrics
        GROUP BY cam_id
        """
    )
    return {row["cam_id"]: row["db_rows"] for row in rows}


async def collect_system_snapshot(redis_client: aioredis.Redis, db_pool: asyncpg.Pool):
    checks = await asyncio.gather(
        *[
            check_tcp_service(name, host, port)
            for name, (host, port) in SERVICE_PORTS.items()
        ]
    )
    healthy_services = sum(1 for item in checks if item["ok"])
    total_services = len(checks)
    db_rows_by_cam = await get_db_rows_by_cam(db_pool)

    per_camera_status = await asyncio.gather(
        *[
            asyncio.gather(
                get_stream_status(redis_client, f"frames:{cam_id}"),
                get_stream_status(redis_client, f"results:{cam_id}"),
            )
            for cam_id in CAMERAS
        ]
    )

    now_ts = datetime.now(timezone.utc)
    async with db_pool.acquire() as conn:
        for cam_id, (frame_stream, result_stream) in zip(CAMERAS, per_camera_status):
            frame_lag_sec = None
            if frame_stream["last_ts"] is not None:
                frame_lag_sec = max(0.0, now_ts.timestamp() - frame_stream["last_ts"])

            result_lag_sec = None
            if result_stream["last_ts"] is not None:
                result_lag_sec = max(0.0, now_ts.timestamp() - result_stream["last_ts"])

            await conn.execute(
                """
                INSERT INTO ice_system_metrics
                (ts, cam_id, healthy_services, total_services,
                 frame_stream_len, result_stream_len, db_rows,
                 frame_lag_sec, result_lag_sec)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                now_ts,
                cam_id,
                healthy_services,
                total_services,
                frame_stream["length"] or 0,
                result_stream["length"] or 0,
                db_rows_by_cam.get(cam_id, 0),
                frame_lag_sec,
                result_lag_sec,
            )


async def system_metrics_writer(redis_client: aioredis.Redis, db_pool: asyncpg.Pool):
    while True:
        try:
            await collect_system_snapshot(redis_client, db_pool)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_event("metrics", f"System snapshot error: {e}", level="error")
        await asyncio.sleep(SYSTEM_METRICS_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.Redis(host=REDIS_HOST, decode_responses=True)
    log_event("api", "Redis client initialized")

    app.state.db = None
    for attempt in range(30):
        try:
            app.state.db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            break
        except Exception as e:
            log_event("api", f"Database not ready yet: {e}", level="warning")
            await asyncio.sleep(3)

    if app.state.db is None:
        raise RuntimeError("Database is unavailable")

    async with app.state.db.acquire() as conn:
        await conn.execute(
            """
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            CREATE TABLE IF NOT EXISTS ice_metrics (
                ts TIMESTAMPTZ NOT NULL,
                cam_id TEXT,
                ice_conc FLOAT,
                ice_severity FLOAT,
                vessel_present BOOLEAN,
                pixels_json TEXT
            );
            SELECT create_hypertable('ice_metrics', 'ts', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS ix_ice_cam ON ice_metrics (cam_id, ts DESC);

            CREATE TABLE IF NOT EXISTS ice_system_metrics (
                ts TIMESTAMPTZ NOT NULL,
                cam_id TEXT NOT NULL,
                healthy_services INT,
                total_services INT,
                frame_stream_len BIGINT,
                result_stream_len BIGINT,
                db_rows BIGINT,
                frame_lag_sec DOUBLE PRECISION,
                result_lag_sec DOUBLE PRECISION
            );
            SELECT create_hypertable('ice_system_metrics', 'ts', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS ix_ice_system_cam ON ice_system_metrics (cam_id, ts DESC);
            """
        )
    log_event("api", "TimescaleDB initialized")

    app.state.frame_readers = [
        asyncio.create_task(frame_reader(cam_id, app.state.redis))
        for cam_id in CAMERAS
    ]
    app.state.result_readers = [
        asyncio.create_task(result_reader(cam_id, app.state.redis, app.state.db))
        for cam_id in CAMERAS
    ]
    app.state.system_metrics_writer = asyncio.create_task(
        system_metrics_writer(app.state.redis, app.state.db)
    )
    log_event("api", f"Pipeline readers started for cameras: {', '.join(CAMERAS)}")

    yield

    for task in app.state.frame_readers + app.state.result_readers + [app.state.system_metrics_writer]:
        task.cancel()
    await app.state.db.close()
    await app.state.redis.close()
    log_event("api", "API shutdown complete", level="warning")


app = FastAPI(lifespan=lifespan, title="Ice Monitor API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "ok", "cameras": CAMERAS}


@app.get("/history/{cam_id}")
async def history(cam_id: str, limit: int = 200):
    if cam_id not in CAMERAS:
        return {"error": "unknown camera"}

    rows = await app.state.db.fetch(
        """
        SELECT ts, ice_conc, ice_severity
        FROM ice_metrics
        WHERE cam_id = $1
        ORDER BY ts DESC
        LIMIT $2
        """,
        cam_id,
        limit,
    )
    return [dict(r) for r in rows]


@app.get("/stats")
async def stats():
    rows = await app.state.db.fetch(
        """
        SELECT cam_id, COUNT(*) AS cnt,
               AVG(ice_conc) AS avg_conc,
               AVG(ice_severity) AS avg_sev
        FROM ice_metrics
        WHERE ts > now() - INTERVAL '1 hour'
        GROUP BY cam_id
        """
    )
    return [dict(r) for r in rows]


@app.get("/pipeline_status")
async def pipeline_status():
    rows = await app.state.db.fetch(
        """
        SELECT cam_id, COUNT(*)::BIGINT AS db_rows, MAX(ts) AS last_db_ts
        FROM ice_metrics
        GROUP BY cam_id
        """
    )
    db_by_cam = {
        row["cam_id"]: {"db_rows": row["db_rows"], "last_db_ts": row["last_db_ts"]}
        for row in rows
    }

    cameras = []
    for cam_id in CAMERAS:
        frame_stream = await get_stream_status(app.state.redis, f"frames:{cam_id}")
        result_stream = await get_stream_status(app.state.redis, f"results:{cam_id}")
        db_info = db_by_cam.get(cam_id, {})

        cameras.append(
            {
                "cam_id": cam_id,
                "frame_stream_len": frame_stream["length"],
                "result_stream_len": result_stream["length"],
                "last_frame_ts": frame_stream["last_ts"],
                "last_result_ts": result_stream["last_ts"],
                "db_rows": db_info.get("db_rows", 0),
                "last_db_ts": db_info.get("last_db_ts"),
                "has_frames": bool(frame_stream["length"]),
                "has_results": bool(result_stream["length"]),
                "has_db_rows": db_info.get("db_rows", 0) > 0,
                "frame_stream_error": frame_stream.get("error"),
                "result_stream_error": result_stream.get("error"),
            }
        )

    return {"status": "ok", "cameras": cameras}


async def check_tcp_service(name: str, host: str, port: int, timeout: float = 1.5):
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        writer.close()
        await writer.wait_closed()
        return {"name": name, "host": host, "port": port, "ok": True}
    except Exception as e:
        return {"name": name, "host": host, "port": port, "ok": False, "error": str(e)}


@app.get("/system_status")
async def system_status():
    checks = await asyncio.gather(
        *[
            check_tcp_service(name, host, port)
            for name, (host, port) in SERVICE_PORTS.items()
        ]
    )
    healthy = sum(1 for item in checks if item["ok"])
    return {
        "status": "ok",
        "healthy": healthy,
        "total": len(checks),
        "services": checks,
    }


@app.get("/logs")
async def logs(limit: int = 100):
    return {"status": "ok", "entries": log_buffer.list(limit)}


async def handle_stream(ws: WebSocket, cam_id: str, manager: ChannelManager):
    if cam_id not in CAMERAS:
        await ws.close(code=1008)
        return

    await manager.connect(ws, cam_id)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, cam_id)
    except Exception:
        manager.disconnect(ws, cam_id)


@app.websocket("/stream/{cam_id}")
async def legacy_result_stream(ws: WebSocket, cam_id: str):
    await handle_stream(ws, cam_id, result_manager)


@app.websocket("/stream/results/{cam_id}")
async def stream_results(ws: WebSocket, cam_id: str):
    await handle_stream(ws, cam_id, result_manager)


@app.websocket("/stream/frames/{cam_id}")
async def stream_frames(ws: WebSocket, cam_id: str):
    await handle_stream(ws, cam_id, frame_manager)
