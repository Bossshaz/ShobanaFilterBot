#  Streaming Server Plugin — MX Player & VLC deep-link integration
#  Provides HTTP Range-aware video streaming directly from Telegram CDN.
#  Routes exposed:
#    GET /stream/{encoded_id}   — binary byte-range stream
#    GET /play/mx/{encoded_id}  — Android intent page for MX Player
#    GET /play/vlc/{encoded_id} — vlc:// deep-link page for VLC

import re
import base64
import logging
from os import environ
from aiohttp import web as webserver

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1MB chunk size — matches Pyrogram's internal streaming block size.
# Do NOT change this; it must align with stream_media's offset/limit units.
# ---------------------------------------------------------------------------
CHUNK_SIZE = 1024 * 1024

# Pyrogram client reference — set by bot.py after the client has started.
_client = None


def set_client(client) -> None:
    global _client
    _client = client


# ---------------------------------------------------------------------------
# URL encoding helpers
# ---------------------------------------------------------------------------
def encode_file_id(file_id: str) -> str:
    """URL-safe base64 encode a Telegram file_id for embedding in paths."""
    return base64.urlsafe_b64encode(file_id.encode()).rstrip(b"=").decode()


def decode_file_id(encoded: str) -> str:
    """Reverse of encode_file_id."""
    padding = (4 - len(encoded) % 4) % 4
    return base64.urlsafe_b64decode(encoded + "=" * padding).decode()


def get_base_url(request: webserver.Request) -> str:
    """Return the public base URL for this server."""
    # Prefer an explicit override, then Replit's domain, then fall back to
    # the Host header (works for local dev and most cloud platforms).
    override = environ.get("STREAM_BASE_URL", "").rstrip("/")
    if override:
        return override
    replit = environ.get("REPLIT_DEV_DOMAIN", "")
    if replit:
        return f"https://{replit}"
    return f"{request.scheme}://{request.host}"


def build_stream_url(file_id: str, request: webserver.Request) -> str:
    return f"{get_base_url(request)}/stream/{encode_file_id(file_id)}"


def build_stream_url_from_env(file_id: str) -> str:
    """Build a stream URL using only env vars (no request context needed)."""
    override = environ.get("STREAM_BASE_URL", "").rstrip("/")
    if override:
        base = override
    else:
        replit = environ.get("REPLIT_DEV_DOMAIN", "")
        if replit:
            base = f"https://{replit}"
        else:
            # Fall back to KEEP_ALIVE_URL (e.g. Koyeb / Render public domain)
            keep_alive = environ.get("KEEP_ALIVE_URL", "").rstrip("/")
            base = keep_alive if keep_alive else ""
    if not base:
        return ""
    return f"{base}/stream/{encode_file_id(file_id)}"


# ---------------------------------------------------------------------------
# Route table (imported and merged into webcode.py's app)
# ---------------------------------------------------------------------------
stream_routes = webserver.RouteTableDef()


# ---------------------------------------------------------------------------
# Route 1: /stream/{encoded_id} — HTTP Range-aware binary stream
# ---------------------------------------------------------------------------
@stream_routes.get("/stream/{encoded_id}")
async def stream_handler(request: webserver.Request) -> webserver.StreamResponse:
    encoded_id = request.match_info["encoded_id"]

    # Decode the file_id
    try:
        file_id = decode_file_id(encoded_id)
    except Exception:
        return webserver.Response(status=400, text="Invalid file ID")

    if _client is None:
        return webserver.Response(status=503, text="Bot client not ready")

    # Fetch file metadata from the database for size and MIME type
    try:
        from database.ia_filterdb import get_file_details
        records = await get_file_details(file_id)
        if not records:
            return webserver.Response(status=404, text="File not found")
        file_doc = records[0]
        file_size: int = int(file_doc.file_size or 0)
        mime_type: str = file_doc.mime_type or "video/mp4"
    except Exception as exc:
        logger.error("Error fetching file details for stream: %s", exc)
        return webserver.Response(status=500, text="Database error")

    if file_size <= 0:
        return webserver.Response(status=500, text="Unknown file size")

    # -----------------------------------------------------------------------
    # Parse HTTP Range header
    # Spec: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
    # -----------------------------------------------------------------------
    range_header = request.headers.get("Range", "")
    start = 0
    end = file_size - 1

    if range_header:
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if m:
            s, e = m.groups()
            start = int(s) if s else 0
            end = int(e) if e else file_size - 1

    # Clamp values
    start = max(0, min(start, file_size - 1))
    end = max(start, min(end, file_size - 1))
    content_length = end - start + 1

    # -----------------------------------------------------------------------
    # Map byte offsets to 1 MB Pyrogram chunk numbers
    # -----------------------------------------------------------------------
    first_chunk = start // CHUNK_SIZE         # index of first 1 MB block
    last_chunk = end // CHUNK_SIZE            # index of last 1 MB block
    num_chunks = last_chunk - first_chunk + 1 # total 1 MB blocks to fetch
    skip_bytes = start % CHUNK_SIZE           # bytes to skip inside first block

    # -----------------------------------------------------------------------
    # Prepare the 206 Partial Content streaming response
    # -----------------------------------------------------------------------
    response = webserver.StreamResponse(
        status=206,
        headers={
            "Content-Type": mime_type,
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(content_length),
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache, no-store",
        },
    )
    await response.prepare(request)

    bytes_sent = 0
    try:
        async for chunk in _client.stream_media(
            file_id, offset=first_chunk, limit=num_chunks
        ):
            # Skip leading bytes inside the first chunk when range starts
            # somewhere in the middle of it.
            if skip_bytes > 0:
                chunk = chunk[skip_bytes:]
                skip_bytes = 0

            # Trim the tail of the last chunk to stay within the range.
            remaining = content_length - bytes_sent
            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            if chunk:
                await response.write(chunk)
                bytes_sent += len(chunk)

            if bytes_sent >= content_length:
                break
    except Exception as exc:
        logger.error("Streaming error for file_id %s: %s", file_id, exc)

    await response.write_eof()
    return response


# ---------------------------------------------------------------------------
# Route 2: /play/mx/{encoded_id} — MX Player Android intent page
# ---------------------------------------------------------------------------
MX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Opening in MX Player…</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; display: flex; flex-direction: column;
           align-items: center; justify-content: center; min-height: 100vh; margin: 0;
           background: #1a1a2e; color: #eee; text-align: center; padding: 20px; }}
    h2  {{ margin-bottom: 8px; }}
    p   {{ color: #aaa; margin-bottom: 24px; }}
    a   {{ display: inline-block; padding: 12px 28px; background: #e94560;
           color: #fff; border-radius: 8px; text-decoration: none; font-weight: 600; }}
    a:hover {{ background: #c73652; }}
  </style>
</head>
<body>
  <h2>▶️ Opening in MX Player…</h2>
  <p>If MX Player doesn't open automatically, tap the button below.</p>
  <a id="link" href="#">Open in MX Player</a>
  <script>
    var streamUrl = "{stream_url}";
    var intent   = "intent://" + streamUrl.replace(/^https?:\/\//, "")
                 + "#Intent;package=com.mxtech.videoplayer.ad;type=video/*;end";
    document.getElementById("link").href = intent;
    setTimeout(function() {{ window.location.href = intent; }}, 500);
  </script>
</body>
</html>"""


@stream_routes.get("/play/mx/{encoded_id}")
async def mx_player_page(request: webserver.Request) -> webserver.Response:
    encoded_id = request.match_info["encoded_id"]
    try:
        file_id = decode_file_id(encoded_id)
    except Exception:
        return webserver.Response(status=400, text="Invalid file ID")

    stream_url = build_stream_url(file_id, request)
    html = MX_HTML.format(stream_url=stream_url)
    return webserver.Response(text=html, content_type="text/html")


# ---------------------------------------------------------------------------
# Route 3: /play/vlc/{encoded_id} — VLC deep-link page
# ---------------------------------------------------------------------------
VLC_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Opening in VLC…</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; display: flex; flex-direction: column;
           align-items: center; justify-content: center; min-height: 100vh; margin: 0;
           background: #1a1a2e; color: #eee; text-align: center; padding: 20px; }}
    h2  {{ margin-bottom: 8px; }}
    p   {{ color: #aaa; margin-bottom: 24px; }}
    a   {{ display: inline-block; padding: 12px 28px; background: #f57c00;
           color: #fff; border-radius: 8px; text-decoration: none; font-weight: 600; }}
    a:hover {{ background: #e65100; }}
  </style>
</head>
<body>
  <h2>▶️ Opening in VLC…</h2>
  <p>If VLC doesn't open automatically, tap the button below.</p>
  <a id="link" href="#">Open in VLC</a>
  <script>
    var streamUrl = "{stream_url}";
    var vlcUrl   = "vlc://" + streamUrl;
    document.getElementById("link").href = vlcUrl;
    setTimeout(function() {{ window.location.href = vlcUrl; }}, 500);
  </script>
</body>
</html>"""


@stream_routes.get("/play/vlc/{encoded_id}")
async def vlc_player_page(request: webserver.Request) -> webserver.Response:
    encoded_id = request.match_info["encoded_id"]
    try:
        file_id = decode_file_id(encoded_id)
    except Exception:
        return webserver.Response(status=400, text="Invalid file ID")

    stream_url = build_stream_url(file_id, request)
    html = VLC_HTML.format(stream_url=stream_url)
    return webserver.Response(text=html, content_type="text/html")
