import asyncio
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import httpx
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

API_URL = "http://91.199.149.128:18001"
CANDIDATE_ID = "bolot"
NSK = ZoneInfo("Asia/Novosibirsk")
BASE_DIR = Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
STATE_FILE = BASE_DIR / "state.json"

DOWNLOADS_DIR.mkdir(exist_ok=True)

app = FastAPI()

files_meta = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}

state = {
    "running": False,
    "finished": False,
    "error": None,
    "start_time": None,
    "names_received": 0,
    "downloaded_of_batch": 0,
    "total_downloaded": len(files_meta),
    "waiting": None,
}


def save_meta():
    STATE_FILE.write_text(json.dumps(files_meta, ensure_ascii=False, indent=2))


async def api_request(client, method, url, **kwargs):
    while True:
        resp = await client.request(method, url, **kwargs)
        if resp.status_code in (429, 403):
            wait = int(resp.headers.get("Retry-After", "5"))
            state["waiting"] = wait
            await asyncio.sleep(wait + 1)
            state["waiting"] = None
            continue
        resp.raise_for_status()
        await asyncio.sleep(0.5)
        return resp


async def download_all():
    state.update(
        running=True,
        finished=False,
        error=None,
        start_time=datetime.now(NSK).strftime("%d.%m.%Y %H:%M:%S"),
        names_received=0,
        downloaded_of_batch=0,
    )
    try:
        headers = {"X-Candidate-Id": CANDIDATE_ID}
        async with httpx.AsyncClient(base_url=API_URL, headers=headers, timeout=60) as client:
            while True:
                resp = await api_request(client, "GET", "/api/files/names")
                names = resp.json()["file_names"]
                if not names:
                    state["finished"] = True
                    break
                state["names_received"] = len(names)
                state["downloaded_of_batch"] = 0
                for i in range(0, len(names), 3):
                    chunk = names[i:i + 3]
                    resp = await api_request(client, "POST", "/api/files/download", json={"file_names": chunk})
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        zf.extractall(DOWNLOADS_DIR)
                    await api_request(client, "POST", "/api/files/downloaded", json={"file_names": chunk})
                    now = datetime.now(NSK).isoformat()
                    for name in chunk:
                        files_meta[name] = now
                    save_meta()
                    state["downloaded_of_batch"] += len(chunk)
                    state["total_downloaded"] = len(files_meta)
    except Exception as e:
        state["error"] = str(e)
    finally:
        state["running"] = False


@app.post("/api/start")
async def start_download():
    if not state["running"]:
        asyncio.create_task(download_all())
    return {"started": True}


@app.get("/api/status")
async def get_status():
    return state


@app.get("/api/downloaded")
async def get_downloaded(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100), order: str = Query("desc")):
    items = sorted(files_meta.items(), key=lambda x: x[1], reverse=(order == "desc"))
    total = len(items)
    start = (page - 1) * page_size
    page_items = [{"name": n, "downloaded_at": t} for n, t in items[start:start + page_size]]
    return {"items": page_items, "total": total, "page": page, "page_size": page_size}


class CalculateRequest(BaseModel):
    file_names: List[str] = []
    all: bool = False


@app.post("/api/calculate")
async def calculate(req: CalculateRequest):
    names = list(files_meta.keys()) if req.all else req.file_names
    digits = "0123456789"
    total = {d: 0 for d in digits}
    per_file = {}
    for name in names:
        path = DOWNLOADS_DIR / name
        if not path.exists():
            continue
        content = path.read_text()
        counts = {d: content.count(d) for d in digits}
        per_file[name] = counts
        for d in digits:
            total[d] += counts[d]
    return {"total": total, "files": per_file}


@app.get("/")
async def index_page():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/files")
async def files_page():
    return FileResponse(BASE_DIR / "static" / "files.html")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
