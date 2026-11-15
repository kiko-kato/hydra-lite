#!/usr/bin/env python3
"""
hydra-dahl-warden: страж пула Dahl Inference для Hydra-Lite.

Запускается НА VPS (systemd timer, каждые 5 минут). Работает с локальным
journalctl и локальной БД /opt/hydra/data/antigravity.db.

Цикл:
  1. Читает журнал dahl-fix с прошлого тика.
  2. "HTTP 429 | returning to client" по ключу #N (429 ушел клиенту) -> suspect.
     3 прокраса за окно -> живая проверка боевым чат-запросом.
  3. 401/403 при живой проверке -> мертв: DELETE из third_party_accounts.
  4. active < MIN_POOL -> добор локальным фармером (farm_dahl_batch.py).
  5. Состояние -> /opt/hydra/dahl-warden_state.json (только JSON, без спама).
"""
import json
import os
import subprocess
import sqlite3
import sys
import time
import urllib.request

DB_PATH = "/opt/hydra/data/antigravity.db"
STATE_PATH = "/opt/hydra/dahl-warden_state.json"
CURSOR_PATH = "/opt/hydra/dahl-warden_cursor"
FARMER = "/opt/hydra/tools/farm_dahl_batch.py"
MIN_POOL = 15
EXTRA = 2
SUSPECT_LIMIT = 3


def log(msg):
    print(f"[WARDEN {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_state():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {"suspects": {}, "last_farm": 0}


def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE_PATH)


def read_cursor():
    try:
        with open(CURSOR_PATH) as f:
            return f.read().strip()
    except Exception:
        return None


def write_cursor(cursor):
    with open(CURSOR_PATH, "w") as f:
        f.write(cursor)


def fetch_journal():
    """Новые строки журнала dahl-fix с прошлой позиции."""
    cur = read_cursor()
    since = cur if cur else "-10 min"
    try:
        out = subprocess.run(
            ["journalctl", "-u", "dahl-fix", "--since", since, "--no-pager", "-o", "json"],
            capture_output=True, text=True, timeout=60).stdout
    except Exception as e:
        log(f"journal fetch fail: {e}")
        return [], None
    entries = []
    last_ts = None
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        ts = e.get("__REALTIME_TIMESTAMP")
        msg = e.get("MESSAGE", "")
        entries.append((int(ts) / 1e6 if ts else 0, msg))
        if ts:
            last_ts = max(last_ts or 0, int(ts))
    return entries, last_ts


def parse_429_final(entries):
    """id ключей, чей 429 ушел клиенту (final)."""
    hit = {}
    for _, msg in entries:
        if "HTTP 429" in msg and "returning to client" in msg:
            try:
                aid = int(msg.split("key #")[1].split()[0])
                hit[aid] = hit.get(aid, 0) + 1
            except Exception:
                pass
    return hit


def pool_rows():
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    rows = con.execute(
        "SELECT id, api_key, status, trial_expires_at FROM third_party_accounts "
        "WHERE provider='dahl'").fetchall()
    con.close()
    return [(int(r[0]), r[1], r[2], r[3]) for r in rows]


def delete_key(api_key):
    try:
        con = sqlite3.connect(DB_PATH, timeout=5)
        con.execute("DELETE FROM third_party_accounts WHERE api_key=? AND provider='dahl'", (api_key,))
        con.commit()
        con.close()
    except Exception as e:
        log(f"delete fail: {e}")


def live_check(api_key):
    """Боевой чат-запрос. True = ключ жив."""
    req = urllib.request.Request(
        "https://inference.dahl.global/v1/chat/completions",
        data=json.dumps({"model": "zai-org/GLM-5.3-Flash",
                         "messages": [{"role": "user", "content": "hi"}],
                         "max_tokens": 2}).encode(),
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        return e.code not in (401, 403)  # 429 = живой (конкарренси), 401/403 = мертв
    except Exception:
        return True  # сеть моргнула — не хороним


def farm(count):
    """Догнать пул фармером. Возвращает число добавленных."""
    log(f"farmer: добираем {count} акков")
    try:
        r = subprocess.run([sys.executable, FARMER, str(count), "3"],
                           capture_output=True, text=True, timeout=900)
        tail = "\n".join(r.stdout.strip().splitlines()[-3:])
        log(f"farmer done: {tail}")
        return tail.count("OK:") if r.returncode == 0 else 0
    except Exception as e:
        log(f"farmer fail: {e}")
        return 0


def main():
    st = load_state()

    entries, last_ts = fetch_journal()
    if last_ts:
        write_cursor(f"{last_ts / 1e6:.3f}")

    final429 = parse_429_final(entries)
    for aid, cnt in final429.items():
        st["suspects"][str(aid)] = st["suspects"].get(str(aid), 0) + cnt

    pool = pool_rows()
    pool_by_id = {r[0]: r for r in pool}

    buried = 0
    for aid_str, cnt in list(st["suspects"].items()):
        aid = int(aid_str)
        if cnt < SUSPECT_LIMIT:
            continue
        if aid not in pool_by_id:
            st["suspects"].pop(aid_str, None)
            continue
        _, key, status, trial = pool_by_id[aid]
        if live_check(key):
            st["suspects"][aid_str] = 0  # живой — обнуляем счетчик
            continue
        log(f"key #{aid} умер (401/403) — удаляю из пула")
        delete_key(key)
        st["suspects"].pop(aid_str, None)
        buried += 1

    pool = pool_rows()
    active = sum(1 for r in pool if r[2] == "active")
    need = (MIN_POOL - active) + (EXTRA if buried else 0)
    if need > 0:
        farmed = farm(need)
        active_after = sum(1 for r in pool_rows() if r[2] == "active")
        log(f"пул: {active} -> {active_after} (farmer +{farmed})")
    else:
        log(f"пул: {active} active, норм (>= {MIN_POOL})")

    st["last_run"] = time.time()
    st["last_active"] = active
    save_state(st)


if __name__ == "__main__":
    main()
