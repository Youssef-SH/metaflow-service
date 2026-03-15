import asyncio
import os

from aiohttp import web

from .api.run import RunApi
from .api.flow import FlowApi

from .api.step import StepApi
from .api.task import TaskApi
from .api.artifact import ArtificatsApi
from .api.admin import AuthApi

from .api.metadata import MetadataApi
from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration

PATH_PREFIX = os.environ.get("PATH_PREFIX", "")
AUDIT_METRICS_PATH = "/__audit/metrics"
AUDIT_RESET_PATH = "/__audit/reset"


def _response_size_bytes(response):
    body_length = getattr(response, "body_length", None)
    if isinstance(body_length, int) and body_length > 0:
        return body_length

    content_length_prop = getattr(response, "content_length", None)
    if isinstance(content_length_prop, int) and content_length_prop > 0:
        return content_length_prop

    if getattr(response, "headers", None):
        content_length = response.headers.get("Content-Length")
        if content_length and content_length.isdigit():
            parsed = int(content_length)
            if parsed > 0:
                return parsed

    body = getattr(response, "body", None)
    if isinstance(body, (bytes, bytearray)):
        return len(body)
    if isinstance(body, str):
        return len(body.encode("utf-8"))

    text = getattr(response, "text", None)
    if isinstance(text, str):
        charset = getattr(response, "charset", None) or "utf-8"
        return len(text.encode(charset))

    raw_body = getattr(response, "_body", None)
    if isinstance(raw_body, (bytes, bytearray)):
        return len(raw_body)
    return 0


async def _request_body_size_bytes(request):
    content_length = request.content_length
    if isinstance(content_length, int) and content_length >= 0:
        return content_length

    cached_body = getattr(request, "_read_bytes", None)
    if isinstance(cached_body, (bytes, bytearray)):
        return len(cached_body)

    # For requests without Content-Length (eg. chunked), read and cache body.
    # aiohttp stores bytes in request._read_bytes, so handlers can still read it.
    body = await request.read()
    return len(body) if isinstance(body, (bytes, bytearray)) else 0


async def _request_size_bytes(request):
    request_line = f"{request.method} {request.path_qs} HTTP/{request.version.major}.{request.version.minor}\r\n"
    request_line_size = len(request_line.encode("utf-8"))

    headers_size = 0
    raw_headers = getattr(request, "raw_headers", None)
    if raw_headers:
        for key, value in raw_headers:
            headers_size += len(key) + 2 + len(value) + 2
    else:
        for key, value in request.headers.items():
            headers_size += len(key.encode("utf-8")) + 2 + len(value.encode("utf-8")) + 2
    headers_size += 2  # header/body delimiter

    body_size = await _request_body_size_bytes(request)
    return request_line_size + headers_size + body_size


def _reset_audit(audit):
    audit["total"] = 0
    audit["response_bytes"] = 0
    audit["request_bytes"] = 0
    audit["by_route"] = {}


@web.middleware
async def audit_middleware(request, handler):
    if request.path.endswith(AUDIT_METRICS_PATH) or request.path.endswith(AUDIT_RESET_PATH):
        return await handler(request)

    audit = request.app["audit"]
    audit["total"] += 1
    audit["request_bytes"] += await _request_size_bytes(request)

    key = f"{request.method} {request.path}"
    audit["by_route"][key] = audit["by_route"].get(key, 0) + 1

    response = await handler(request)
    # Approximate response payload bytes served by metadata service.
    audit["response_bytes"] += _response_size_bytes(response)
    return response


async def audit_metrics(request):
    return web.json_response(request.app["audit"])


async def audit_reset(request):
    _reset_audit(request.app["audit"])
    return web.json_response({"ok": True})


def app(loop=None, db_conf: DBConfiguration = None, middlewares=None, path_prefix=""):

    loop = loop or asyncio.get_event_loop()

    _app = web.Application()
    app = web.Application() if path_prefix else _app
    app["audit"] = {
        "total": 0,
        "response_bytes": 0,
        "request_bytes": 0,
        "by_route": {}
    }
    app.middlewares.append(audit_middleware)
    async_db = AsyncPostgresDB()
    loop.run_until_complete(async_db._init(db_conf))
    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    MetadataApi(app)
    ArtificatsApi(app)
    AuthApi(app)
    app.router.add_get(AUDIT_METRICS_PATH, audit_metrics)
    app.router.add_post(AUDIT_RESET_PATH, audit_reset)

    if path_prefix:
        _app.add_subapp(path_prefix, app)
    if middlewares:
        _app.middlewares.extend(middlewares)
    return _app


def main():
    loop = asyncio.get_event_loop()
    the_app = app(loop, DBConfiguration(), path_prefix=PATH_PREFIX)
    handler = web.AppRunner(the_app)
    loop.run_until_complete(handler.setup())

    port = os.environ.get("MF_METADATA_PORT", 8080)
    host = str(os.environ.get("MF_METADATA_HOST", "0.0.0.0"))
    f = loop.create_server(handler.server, host, port)

    srv = loop.run_until_complete(f)
    print("serving on", srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
