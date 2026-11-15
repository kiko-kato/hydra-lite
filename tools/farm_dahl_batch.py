#!/usr/bin/env python3
"""
Batch registration for Dahl Inference (https://inference.dahl.global/).
Spawns an isolated ephemeral sing-box instance on an ephemeral port.
Registers N accounts, allocates 100M tokens each, and saves them to keys.db (dahl_pool).
"""

import json
import subprocess
import time
import os
import sys
import random
import string
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
from curl_cffi import requests

KEYS_DB_PATH = "/opt/hydra/data/keys.db"
SITE_KEY = "0x4AAAAAAFo5t2cRvAgWO-5T"
PAGE_URL = "https://inference.dahl.global/account"

db_lock = threading.Lock()


def get_2captcha_key():
    conn = sqlite3.connect(KEYS_DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT value FROM keys WHERE service = '2captcha'")
    row = cur.fetchone()
    conn.close()
    if not row:
        raise RuntimeError("2captcha key not found in keys.db")
    return row[0]


def solve_turnstile(api_key):
    payload = {
        'clientKey': api_key,
        'task': {
            'type': 'TurnstileTaskProxyless',
            'websiteURL': PAGE_URL,
            'websiteKey': SITE_KEY,
            'action': 'signup'
        }
    }
    req = urllib.request.Request(
        'https://api.2captcha.com/createTask',
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req, timeout=12) as r:
        res = json.loads(r.read())
        task_id = res.get('taskId')
        if not task_id:
            raise RuntimeError(f"Failed to create 2captcha task: {res}")

    for _ in range(40):
        time.sleep(2)
        req_res = urllib.request.Request(
            'https://api.2captcha.com/getTaskResult',
            data=json.dumps({'clientKey': api_key, 'taskId': task_id}).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req_res, timeout=12) as r_res:
            res_poll = json.loads(r_res.read())
            status = res_poll.get('status')
            if status == 'ready':
                return res_poll.get('solution', {}).get('token')
            elif status == 'failed':
                raise RuntimeError(f"2captcha task failed: {res_poll}")

    raise TimeoutError("Turnstile solving timed out")


def save_to_db(username, fingerprint, api_token, tokens, trial_expires_at):
    with db_lock:
        # Локальная БД Hydra-Lite (фармер запускается на VPS)
        try:
            con = sqlite3.connect("/opt/hydra/data/antigravity.db")
            con.execute(
                "INSERT OR IGNORE INTO third_party_accounts (provider, username, fingerprint, api_key, tokens, trial_expires_at, status) "
                "VALUES ('dahl', ?, ?, ?, ?, ?, 'active')",
                (username, fingerprint, api_token, tokens, trial_expires_at))
            con.commit()
            con.close()
        except Exception as e:
            print(f"[!] Warning local db sync: {e}")

        # Зеркало в локальный keys.db (если есть)
        try:
            conn = sqlite3.connect(KEYS_DB_PATH)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO dahl_pool (username, fingerprint, key, tokens, trial_expires_at)
                VALUES (?, ?, ?, ?, ?)
            """, (username, fingerprint, api_token, tokens, trial_expires_at))
            conn.commit()
            conn.close()
        except Exception:
            pass


def register_single_account(idx, captcha_key, max_retries=2):
    # VPS уже за границей (US) — прокси не нужен, прямой выход
    session = requests.Session(impersonate="chrome124")

    for attempt in range(max_retries + 1):
        try:
            rand_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
            username = f"kiko_{rand_suffix}"

            # 1. Solve captcha
            turnstile_token = solve_turnstile(captcha_key)

            # 2. Session (прямой выход с VPS, без прокси)
            session.get("https://inference.dahl.global/account", timeout=12)

            # 3. Signup
            signup_payload = {
                'username': username,
                'turnstile_token': turnstile_token
            }
            headers = {
                'Origin': 'https://inference.dahl.global',
                'Referer': 'https://inference.dahl.global/account',
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/plain, */*'
            }
            resp = session.post(
                "https://inference.dahl.global/v1/auth/signup",
                json=signup_payload,
                headers=headers,
                timeout=15
            )

            if resp.status_code != 201:
                print(f"[!] [{idx}] Signup status {resp.status_code}: {resp.text[:100]}")
                time.sleep(2)
                continue

            data = resp.json()
            user_info = data.get('user', {})
            api_key_info = data.get('api_key', {})
            fingerprint = data.get('fingerprint')
            key_id = api_key_info.get('id')
            api_token = api_key_info.get('token')

            # 4. Check pool balance and allocate
            acc_resp = session.get("https://inference.dahl.global/v1/account", timeout=12)
            acc_data = acc_resp.json()
            token_balance = acc_data.get('token_balance', 0)
            trial_expires_at = acc_data.get('trial_expires_at', '')

            if token_balance > 0 and key_id:
                session.post(
                    "https://inference.dahl.global/v1/account/allocate",
                    json={'amount': token_balance, 'key_id': key_id},
                    headers={
                        'Origin': 'https://inference.dahl.global',
                        'Referer': 'https://inference.dahl.global/account',
                        'Content-Type': 'application/json'
                    },
                    timeout=12
                )

            save_to_db(username, fingerprint, api_token, token_balance, trial_expires_at)
            print(f"[+] [{idx}] OK: {username} | key: {api_token[:14]}... | {token_balance//1000000}M tokens")
            return {
                'idx': idx,
                'username': username,
                'fingerprint': fingerprint,
                'key': api_token,
                'tokens': token_balance,
                'trial_expires_at': trial_expires_at
            }

        except Exception as e:
            print(f"[-] [{idx}] Attempt {attempt+1} error: {e}")
            time.sleep(2)

    print(f"[x] [{idx}] Failed after {max_retries+1} attempts.")
    return None


def run_batch(total_target=16, concurrency=3):
    captcha_key = get_2captcha_key()

    print(f"[*] Starting batch registration: {total_target} accounts, {concurrency} workers (direct, no proxy).")
    results = []

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(register_single_account, i + 1, captcha_key)
            for i in range(total_target)
        ]
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    print(f"\n[DONE] Successfully registered {len(results)} of {total_target} accounts.")
    return results


if __name__ == '__main__':
    target = int(sys.argv[1]) if len(sys.argv) > 1 else 16
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    run_batch(target, workers)
