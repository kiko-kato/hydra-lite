#!/usr/bin/env python3
"""
dahl-tools-fix: fronting proxy for hydra-lite.
Fixes: hydra-lite strips `tools`/`tool_choice` when forwarding requests to the
3PA "Dahl Inference" upstream (inference.dahl.global). This proxy intercepts
dahl-model requests, forwards the FULL client body (tools included) to dahl
directly using pool keys from hydra's DB, and relays everything else to
hydra-lite (127.0.0.1:1024) verbatim, including SSE streaming.
"""
import asyncio
import json
import re
import sqlite3
import ssl
import time

LISTEN_HOST, LISTEN_PORT = "127.0.0.1", 1030
HYDRA_HOST, HYDRA_PORT = "127.0.0.1", 1024
DAHL_HOST, DAHL_PORT = "inference.dahl.global", 443
ACCOUNTS_DB = "/opt/hydra/data/antigravity.db"
CONFIG_PATH = "/opt/hydra/config.toml"

CANONICAL = {
    "deepseek-ai/DeepSeek-V4-Flash-0731",
    "zai-org/GLM-5.3-Flash",
}
STATIC_ALIASES = {
    "dahl-deepseek-v4-flash": "deepseek-ai/DeepSeek-V4-Flash-0731",
    "dahl-glm-5.3-flash": "zai-org/GLM-5.3-Flash",
}

MAX_ATTEMPTS = 10
CONNECT_TIMEOUT = 15
IDLE_TIMEOUT = 600
BODY_LIMIT = 64 * 1024 * 1024
COOLDOWNS = {429: 8, 401: 600, 403: 600, 500: 10, 502: 10, 503: 10}
REFRESH_SEC = 60
CHROME_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HOP_HEADERS = {"connection", "keep-alive", "transfer-encoding", "upgrade",
               "proxy-connection", "te", "trailers"}


def log(msg: str) -> None:
    print(f"[DAHLFIX {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}", flush=True)


def read_master_key() -> str:
    try:
        text = open(CONFIG_PATH).read()
        m = re.search(r'api_key\s*=\s*"([^"]+)"', text)
        if m:
            return m.group(1)
    except Exception as e:
        log(f"config read failed: {e}")
    return ""


class State:
    def __init__(self):
        self.master_key = read_master_key()
        self.keys = []            # [{"id": int, "key": str}]
        self.cooldown = {}        # key_str -> unix_ts
        self.rr = 0               # round-robin pointer
        self.alias_map = dict(STATE_ALIASES)

    def available_keys(self):
        now = time.time()
        return [k for k in self.keys if self.cooldown.get(k["key"], 0) <= now]

    def next_key(self):
        avail = self.available_keys()
        if not avail:
            return None
        k = avail[self.rr % len(avail)]
        self.rr += 1
        return k


STATE_ALIASES = {}  # replaced by State init after definition
STATE = State()
STATE.alias_map.update(STATIC_ALIASES)


def is_trial_valid(ts: str) -> bool:
    if not ts:
        return True
    try:
        t = time.mktime(time.strptime(ts.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z"))
        return t > time.time()
    except Exception:
        return True


def refresh_keys():
    try:
        con = sqlite3.connect(f"file:{ACCOUNTS_DB}?mode=ro", uri=True, timeout=5)
        rows = con.execute(
            "select id, api_key, status, trial_expires_at from third_party_accounts "
            "where provider='dahl'").fetchall()
        con.close()
        keys = [{"id": r[0], "key": r[1]} for r in rows
                if r[1] and r[2] == "active" and is_trial_valid(r[3])]
        STATE.keys = keys
        log(f"pool refresh: {len(keys)} usable accounts")
    except Exception as e:
        log(f"pool refresh FAILED: {e}")


def client_key_ok(auth_header: str, x_api_key: str) -> bool:
    key = ""
    if auth_header and auth_header.lower().startswith("bearer "):
        key = auth_header[7:].strip()
    elif x_api_key:
        key = x_api_key.strip()
    if not key:
        return False
    if key == STATE.master_key:
        return True
    try:
        con = sqlite3.connect(f"file:{ACCOUNTS_DB}?mode=ro", uri=True, timeout=5)
        row = con.execute(
            "select enabled, expires_at from api_keys where key=?", (key,)).fetchone()
        con.close()
    except Exception as e:
        log(f"api_keys check failed: {e}")
        return False
    if not row or not row[0]:
        return False
    exp = row[1]
    if exp:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                if time.mktime(time.strptime(exp.replace("Z", "+0000"), fmt)) < time.time():
                    return False
                break
            except Exception:
                continue
    return True


async def refresh_models():
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HYDRA_HOST, HYDRA_PORT), CONNECT_TIMEOUT)
        req = (f"GET /v1/models HTTP/1.1\r\nHost: {HYDRA_HOST}:{HYDRA_PORT}\r\n"
               f"Authorization: Bearer {STATE.master_key}\r\n"
               f"Accept: */*\r\nConnection: close\r\n\r\n")
        writer.write(req.encode())
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(4 * 1024 * 1024), 30)
        writer.close()
        head, _, body = raw.partition(b"\r\n\r\n")
        data = json.loads(body.decode("utf-8", "replace"))
        amap = {}
        for item in data.get("data", []):
            mid = item.get("id", "")
            norm = mid[5:] if mid.startswith("dahl/") else mid
            for c in CANONICAL:
                if norm.lower() == c.lower():
                    amap[mid] = c
                    amap[mid.lower()] = c
        amap.update(STATIC_ALIASES)
        STATE.alias_map = amap
        log(f"alias map refreshed: {len(amap)} entries")
    except Exception as e:
        log(f"models refresh failed (keeping old map): {e}")


async def refresh_loop():
    while True:
        try:
            await asyncio.get_running_loop().run_in_executor(None, refresh_keys)
            await refresh_models()
        except Exception as e:
            log(f"refresh loop error: {e}")
        await asyncio.sleep(REFRESH_SEC)


def resolve_dahl(model):
    if not isinstance(model, str) or not model:
        return None
    amap = STATE.alias_map
    if model in amap:
        return amap[model]
    lm = model.lower()
    if lm in amap:
        return amap[lm]
    if model.startswith("dahl/"):
        n = model[5:]
        for c in CANONICAL:
            if n.lower() == c.lower():
                return c
    return None


# ---------------------------------------------------------------- HTTP plumbing
async def read_request(reader):
    line = await reader.readline()
    if not line:
        return None
    try:
        method, target, version = line.decode("latin1").strip().split(" ", 2)
    except ValueError:
        return None
    headers = {}
    while True:
        raw = await reader.readline()
        if raw in (b"\r\n", b"\n", b"", b"\r"):
            break
        k, _, v = raw.decode("latin1").partition(":")
        headers.setdefault(k.strip().lower(), v.strip())
    body = b""
    te = headers.get("transfer-encoding", "").lower()
    if "chunked" in te:
        parts = []
        while True:
            szline = await reader.readline()
            try:
                size = int(szline.split(b";")[0].strip() or b"0", 16)
            except ValueError:
                break
            if size == 0:
                while True:
                    t = await reader.readline()
                    if t in (b"\r\n", b"\n", b"", b"\r"):
                        break
                break
            parts.append(await reader.readexactly(size))
            await reader.readexactly(2)
        body = b"".join(parts)
    elif "content-length" in headers:
        body = await reader.readexactly(int(headers["content-length"]))
    return method, target, version, headers, body


async def read_message(reader, headers):
    """Read a full response/request body per framing. Returns decoded bytes."""
    cl = headers.get("content-length")
    te = headers.get("transfer-encoding", "").lower()
    if cl is not None:
        return await reader.readexactly(int(cl))
    if "chunked" in te:
        parts = []
        while True:
            szline = await reader.readline()
            try:
                size = int(szline.split(b";")[0].strip() or b"0", 16)
            except ValueError:
                break
            if size == 0:
                while True:
                    t = await reader.readline()
                    if t in (b"\r\n", b"\n", b"", b"\r"):
                        break
                break
            parts.append(await reader.readexactly(size))
            await reader.readexactly(2)
        return b"".join(parts)
    return await reader.read()


def send_response(writer, status, reason, headers, body, is_head=False):
    lines = [f"HTTP/1.1 {status} {reason}\r\n"]
    has_cl = False
    for k, v in headers.items():
        if k.lower() == "content-length":
            has_cl = True
        lines.append(f"{k}: {v}\r\n")
    if not has_cl:
        lines.append(f"Content-Length: {len(body)}\r\n")
    lines.append("Connection: close\r\n\r\n")
    writer.write("".join(lines).encode("latin1") + (b"" if is_head else body))


async def pump_decoded(up_reader, status, reason, up_headers, client_writer):
    """Stream a DECODED body to the client (close-delimited), handling
    content-length / chunked / EOF framing from the upstream."""
    hdrs = {"Content-Type": up_headers.get("content-type", "application/octet-stream"),
            "Cache-Control": "no-cache", "Connection": "close"}
    out = [f"HTTP/1.1 {status} {reason or 'OK'}\r\n"]
    for k, v in hdrs.items():
        out.append(f"{k}: {v}\r\n")
    out.append("\r\n")
    client_writer.write("".join(out).encode("latin1"))
    await client_writer.drain()
    cl = up_headers.get("content-length")
    te = up_headers.get("transfer-encoding", "").lower()
    if cl is not None:
        left = int(cl)
        while left > 0:
            chunk = await asyncio.wait_for(up_reader.read(min(16384, left)), IDLE_TIMEOUT)
            if not chunk:
                break
            left -= len(chunk)
            client_writer.write(chunk)
            await client_writer.drain()
    elif "chunked" in te:
        while True:
            szline = await asyncio.wait_for(up_reader.readline(), IDLE_TIMEOUT)
            try:
                size = int(szline.split(b";")[0].strip() or b"0", 16)
            except ValueError:
                break
            if size == 0:
                break
            data = await asyncio.wait_for(up_reader.readexactly(size), IDLE_TIMEOUT)
            await up_reader.readexactly(2)
            client_writer.write(data)
            await client_writer.drain()
    else:
        while True:
            chunk = await asyncio.wait_for(up_reader.read(16384), IDLE_TIMEOUT)
            if not chunk:
                break
            client_writer.write(chunk)
            await client_writer.drain()


async def parse_response_head(reader):
    status_line = await reader.readline()
    try:
        _, status, reason = status_line.decode("latin1").strip().split(" ", 2)
    except ValueError:
        _, status = status_line.decode("latin1").strip().split(" ", 1)
        reason = ""
    headers = {}
    while True:
        raw = await reader.readline()
        if raw in (b"\r\n", b"\n", b"", b"\r"):
            break
        k, _, v = raw.decode("latin1").partition(":")
        headers.setdefault(k.strip().lower(), v.strip())
    return int(status), reason, headers


# ---------------------------------------------------------------- dahl upstream
async def dahl_open(key):
    ctx = ssl.create_default_context()
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(DAHL_HOST, DAHL_PORT, ssl=ctx), CONNECT_TIMEOUT)
    return reader, writer


async def dahl_send(writer, key, body_bytes):
    hdrs = {
        "Host": DAHL_HOST,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": CHROME_UA,
        "Accept-Encoding": "identity",
        "Content-Length": str(len(body_bytes)),
        "Connection": "close",
    }
    req = f"POST /v1/chat/completions HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items()) + "\r\n"
    writer.write(req.encode("latin1") + body_bytes)
    await writer.drain()


def json_error(status, code, message):
    body = json.dumps({"error": {"message": message, "code": code,
                                 "type": "upstream_error" if status >= 500 else "invalid_request_error"}}).encode()
    return status, body


# ---------------------------------------------------------------- handlers
async def handle_dahl(target, headers, body, client_writer):
    t0 = time.time()
    if not client_key_ok(headers.get("authorization"), headers.get("x-api-key")):
        st, body_out = json_error(401, "invalid_api_key",
                                  "Invalid or missing API key. Keys are configured in Hydra.")
        send_response(client_writer, st, "Unauthorized", {"Content-Type": "application/json"}, body_out)
        return
    try:
        payload = json.loads(body.decode("utf-8", "replace"))
    except Exception:
        st, body_out = json_error(400, "bad_json", "Request body is not valid JSON.")
        send_response(client_writer, st, "Bad Request", {"Content-Type": "application/json"}, body_out)
        return

    model = payload.get("model")
    canonical = resolve_dahl(model)
    if not canonical:
        st, body_out = json_error(400, "unknown_model",
                                  f"Model '{model}' is not a known dahl model.")
        send_response(client_writer, st, "Bad Request", {"Content-Type": "application/json"}, body_out)
        return
    payload["model"] = canonical
    if "stream" not in payload:
        payload["stream"] = False
    upstream_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    want_stream = bool(payload.get("stream"))
    has_tools = "tools" in payload and payload["tools"]

    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        acct = STATE.next_key()
        if acct is None:
            break
        key = acct["key"]
        try:
            reader, writer = await dahl_open(key)
        except Exception as e:
            STATE.cooldown[key] = time.time() + 10
            last_err = f"connect: {e}"
            log(f"key #{acct['id']} connect fail: {e}")
            continue
        try:
            await dahl_send(writer, key, upstream_body)
            status, reason, rh = await parse_response_head(reader)
        except Exception as e:
            STATE.cooldown[key] = time.time() + 10
            last_err = f"request: {e}"
            log(f"key #{acct['id']} send/parse fail: {e}")
            try:
                writer.close()
            except Exception:
                pass
            continue

        if status == 200:
            try:
                if want_stream:
                    log(f"key #{acct['id']} | {canonical} | tools={bool(has_tools)} "
                        f"stream=true | 200 | pumping")
                    await pump_decoded(reader, status, reason, rh, client_writer)
                else:
                    resp_body = await asyncio.wait_for(read_message(reader, rh), IDLE_TIMEOUT)
                    ms = int((time.time() - t0) * 1000)
                    usage = ""
                    try:
                        u = json.loads(resp_body).get("usage", {})
                        usage = f" | usage {u.get('prompt_tokens')}/{u.get('completion_tokens')}/{u.get('total_tokens')}"
                    except Exception:
                        pass
                    log(f"key #{acct['id']} | {canonical} | tools={bool(has_tools)} "
                        f"stream=false | 200 | {ms}ms{usage}")
                    send_response(client_writer, 200, "OK",
                                  {"Content-Type": rh.get("content-type", "application/json")}, resp_body)
            except asyncio.TimeoutError:
                log(f"key #{acct['id']} upstream idle timeout, closing")
            except Exception as e:
                log(f"key #{acct['id']} relay aborted: {e}")
            finally:
                try:
                    writer.close()
                except Exception:
                    pass
                return

        # non-200 from dahl: read error body, maybe rotate
        try:
            err_body = await asyncio.wait_for(read_message(reader, rh), 30)
        except Exception:
            err_body = b""
        finally:
            try:
                writer.close()
            except Exception:
                pass
        cd = COOLDOWNS.get(status)
        if cd is not None and attempt < MAX_ATTEMPTS - 1:
            STATE.cooldown[key] = time.time() + cd
            last_err = f"HTTP {status}: {err_body[:200]!r}"
            log(f"key #{acct['id']} HTTP {status} -> cooldown {cd}s, rotating")
            if status == 429:
                await asyncio.sleep(0.3 * (attempt + 1))
            continue
        # non-retryable or last attempt: pass through to client verbatim
        log(f"key #{acct['id']} | {canonical} | HTTP {status} | returning to client")
        send_response(client_writer, status, reason or "Error",
                      {"Content-Type": rh.get("content-type", "application/json")}, err_body)
        return

    ms = int((time.time() - t0) * 1000)
    log(f"pool exhausted for {canonical} | {ms}ms | last_err={last_err}")
    st, body_out = json_error(502, "3pa_pool_exhausted",
                              "All dahl pool accounts failed to respond or are rate-limited.")
    send_response(client_writer, st, "Bad Gateway", {"Content-Type": "application/json"}, body_out)


async def relay_hydra(method, target, headers, body, client_writer):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HYDRA_HOST, HYDRA_PORT), CONNECT_TIMEOUT)
    except Exception as e:
        st, body_out = json_error(502, "gateway_down", f"hydra-lite unreachable: {e}")
        send_response(client_writer, st, "Bad Gateway", {"Content-Type": "application/json"}, body_out)
        return
    try:
        fwd = {k: v for k, v in headers.items() if k not in HOP_HEADERS and k != "host"}
        lines = [f"{method} {target} HTTP/1.1\r\n", f"Host: {HYDRA_HOST}:{HYDRA_PORT}\r\n"]
        for k, v in fwd.items():
            lines.append(f"{k}: {v}\r\n")
        lines.append(f"Content-Length: {len(body)}\r\n")
        lines.append("Connection: close\r\n\r\n")
        writer.write("".join(lines).encode("latin1") + body)
        await writer.drain()
        status, reason, rh = await parse_response_head(reader)
        cl = rh.get("content-length")
        te = rh.get("transfer-encoding", "").lower()
        if method == "HEAD":
            out_headers = {k: v for k, v in rh.items() if k not in HOP_HEADERS}
            send_response(client_writer, status, reason, out_headers, b"", is_head=True)
            return
        small = cl is not None and int(cl) <= 1024 * 1024
        if small:
            resp_body = await asyncio.wait_for(read_message(reader, rh), IDLE_TIMEOUT)
            out_headers = {k: v for k, v in rh.items() if k not in HOP_HEADERS}
            send_response(client_writer, status, reason, out_headers, resp_body)
        else:
            # stream (SSE or large): decode framing, pipe close-delimited
            await pump_decoded(reader, status, reason, rh, client_writer)
    except asyncio.TimeoutError:
        log(f"hydra relay idle timeout on {method} {target}")
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        log(f"hydra relay error on {method} {target}: {e}")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle_client(reader, writer):
    try:
        req = await asyncio.wait_for(read_request(reader), 60)
        if req is None:
            return
        method, target, version, headers, body = req
        if len(body) > BODY_LIMIT:
            st, body_out = json_error(413, "payload_too_large", "Payload too large")
            send_response(writer, st, "Payload Too Large", {"Content-Type": "application/json"}, body_out)
            return
        if method == "GET" and target.startswith("/health"):
            send_response(writer, 200, "OK", {"Content-Type": "text/plain"}, b"ok\n")
            return
        if method == "POST" and target.startswith("/v1/chat/completions"):
            try:
                model = (json.loads(body.decode("utf-8", "replace")) or {}).get("model")
            except Exception:
                model = None
            if resolve_dahl(model):
                await handle_dahl(target, headers, body, writer)
                return
        await relay_hydra(method, target, headers, body, writer)
    except asyncio.TimeoutError:
        log("client request read timeout")
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        pass
    except Exception as e:
        log(f"handler error: {e!r}")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    log(f"starting dahl-tools-fix on {LISTEN_HOST}:{LISTEN_PORT} "
        f"(hydra={HYDRA_HOST}:{HYDRA_PORT}, dahl={DAHL_HOST}:{DAHL_PORT})")
    await asyncio.get_running_loop().run_in_executor(None, refresh_keys)
    await refresh_models()
    asyncio.create_task(refresh_loop())
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
