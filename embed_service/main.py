"""Zeppelin Embed Sidecar — lightweight text→vector encoding service.

Hosts all-MiniLM-L6-v2 (384-dim, 22M params) for hackathon users.
Rate-limited per IP, max 10 texts × 512 chars per request.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("embed_service")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_NAME = "all-MiniLM-L6-v2"
MAX_TEXTS = 10
MAX_TEXT_LENGTH = 512
RATE_LIMIT_RPS = 5  # per IP
RATE_LIMIT_BURST = 10

# ---------------------------------------------------------------------------
# Rate limiter (token bucket, per IP)
# ---------------------------------------------------------------------------
_buckets: Dict[str, List[float]] = defaultdict(lambda: [float(RATE_LIMIT_BURST), time.monotonic()])


def _check_rate_limit(ip: str) -> bool:
    bucket = _buckets[ip]
    now = time.monotonic()
    elapsed = now - bucket[1]
    refill = elapsed * RATE_LIMIT_RPS
    if refill >= 1.0:
        bucket[0] = min(bucket[0] + refill, float(RATE_LIMIT_BURST))
        bucket[1] = now
    if bucket[0] >= 1.0:
        bucket[0] -= 1.0
        return True
    return False


# ---------------------------------------------------------------------------
# App lifecycle — load model once at startup
# ---------------------------------------------------------------------------
model: Optional[SentenceTransformer] = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global model
    print(f"Loading model {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    print(f"Model loaded: {MODEL_NAME} ({model.get_sentence_embedding_dimension()}d)")
    yield
    model = None


app = FastAPI(title="Zeppelin Embed Service", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    start = time.monotonic()
    response = await call_next(request)
    latency_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "ip=%s method=%s path=%s status=%d latency_ms=%d",
        ip, request.method, request.url.path, response.status_code, latency_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------
class EmbedRequest(BaseModel):
    texts: List[str] = Field(..., min_length=1, max_length=MAX_TEXTS)


class EmbedResponse(BaseModel):
    embeddings: List[List[float]]
    model: str
    dimensions: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/v1/embed", response_model=EmbedResponse)
async def embed(request: Request, body: EmbedRequest):
    # Per-IP rate limiting
    ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(ip):
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded, retry after 1s",
            headers={"Retry-After": "1"},
        )

    # Validate individual text lengths
    for i, text in enumerate(body.texts):
        if len(text) > MAX_TEXT_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"text[{i}] exceeds {MAX_TEXT_LENGTH} character limit ({len(text)} chars)",
            )
        if not text.strip():
            raise HTTPException(status_code=400, detail=f"text[{i}] is empty or whitespace")

    # Encode
    embeddings = model.encode(body.texts, normalize_embeddings=True)
    return EmbedResponse(
        embeddings=embeddings.tolist(),
        model=MODEL_NAME,
        dimensions=embeddings.shape[1],
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": MODEL_NAME, "ready": model is not None}


@app.get("/")
async def root():
    return {"service": "zeppelin-embed", "model": MODEL_NAME, "endpoints": ["/v1/embed", "/health"]}
