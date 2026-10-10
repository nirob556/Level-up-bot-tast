# -*- coding: utf-8 -*-
"""
============================================================
  FF Level Up Bot - ULTIMATE EDITION
  Created by: NIROB
  Version: 3.0.0
============================================================
"""

import sys
import os

os.environ['PYTHONUNBUFFERED'] = '1'
os.environ.setdefault('PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION', 'python')

try:
    sys.stdout.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
except Exception:
    pass

import asyncio
import httpx
import random
import json
import socket
import struct
import time
import uuid
import itertools
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from collections import deque

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from google_play_scraper import app as play_scraper
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

try:
    from protobuf_decoder.protobuf_decoder import Parser
    HAS_PROTO_DECODER = True
except ImportError:
    HAS_PROTO_DECODER = False
    Parser = None

try:
    from message_ids import MESSAGE_ID_TO_NAME
except ImportError:
    MESSAGE_ID_TO_NAME = {}

try:
    import thunderFF_pb2
except ImportError:
    thunderFF_pb2 = None


# ============================================================
#  CONFIGURATION
# ============================================================

START_MATCH_INTERVAL = 1
MAX_MATCH_DURATION = 1500
MATCH_IDLE_TIMEOUT = 2
NON_MATCH_RECONNECT_DELAY = 0.5
MODE_ID = 1
MAP_ID = 1

AES_KEY = b'Yg&tc%DEuh6%Zc^8'
AES_IV = b'6oyZDr22E3ychjM%'

CLOUDFLARE_PRIMARY_DNS = "1.1.1.1"
CLOUDFLARE_SECONDARY_DNS = "1.0.0.1"
_DNS_CACHE: Dict[str, Tuple[str, float]] = {}
_DNS_CACHE_TTL = 300.0

_DELTA = 0x9E3779B9
_ROUNDS = 16
_FIELD_SIZES = {0: 1, 1: 2, 2: 2, 3: 1, 4: 2}
_FIELD_NAMES = {0: "sendOption", 1: "cmd", 2: "orderId", 3: "flags", 4: "length"}

FALLBACK_GETLOGIN_URL = "https://clientbp.ggpolarbear.com"

CRC7_TABLE = bytes([
    0, 9, 18, 27, 36, 45, 54, 63, 72, 65, 90, 83, 108, 101, 126, 119,
    25, 16, 11, 2, 61, 52, 47, 38, 81, 88, 67, 74, 117, 124, 103, 110,
    50, 59, 32, 41, 22, 31, 4, 13, 122, 115, 104, 97, 94, 87, 76, 69,
    43, 34, 57, 48, 15, 6, 29, 20, 99, 106, 113, 120, 71, 78, 85, 92,
    100, 109, 118, 127, 64, 73, 82, 91, 44, 37, 62, 55, 8, 1, 26, 19,
    125, 116, 111, 102, 89, 80, 75, 66, 53, 60, 39, 46, 17, 24, 3, 10,
    86, 95, 68, 77, 114, 123, 96, 105, 30, 23, 12, 5, 58, 51, 40, 33,
    79, 70, 93, 84, 107, 98, 121, 112, 7, 14, 21, 28, 35, 42, 49, 56,
    65, 72, 83, 90, 101, 108, 119, 126, 9, 0, 27, 18, 45, 36, 63, 54,
    88, 81, 74, 67, 124, 117, 110, 103, 16, 25, 2, 11, 52, 61, 38, 47,
    115, 122, 97, 104, 87, 94, 69, 76, 59, 50, 41, 32, 31, 22, 13, 4,
    106, 99, 120, 113, 78, 71, 92, 85, 34, 43, 48, 57, 6, 15, 20, 29,
    37, 44, 55, 62, 1, 8, 19, 26, 109, 100, 127, 118, 73, 64, 91, 82,
    60, 53, 46, 39, 24, 17, 10, 3, 116, 125, 102, 111, 80, 89, 66, 75,
    23, 30, 5, 12, 51, 58, 33, 40, 95, 86, 77, 68, 123, 114, 105, 96,
    14, 7, 28, 21, 42, 35, 56, 49, 70, 79, 84, 93, 98, 107, 112, 121,
])


# ============================================================
#  BROADCAST HUB
# ============================================================

class BroadcastHub:
    def __init__(self):
        self.clients: List[WebSocket] = []
        self.log_history = deque(maxlen=1000)
        self.stats = {
            "accounts": {},
            "total_matches": 0,
            "active_matches": 0,
            "uptime_start": 0,
            "bot_status": "idle",
            "session_matches": 0,
            "success_rate": 0,
            "total_logins": 0,
        }
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.clients.append(ws)
        try:
            await ws.send_json({
                "type": "history",
                "data": list(self.log_history),
                "stats": self.stats
            })
        except Exception:
            pass

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            if ws in self.clients:
                self.clients.remove(ws)

    async def broadcast(self, message: dict):
        if message.get("type") == "log":
            self.log_history.append(message.get("data", {}))
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in self.clients:
                self.clients.remove(ws)

    async def update_stats(self):
        await self.broadcast({"type": "stats", "data": self.stats})

    def log_sync(self, level: str, message: str, account: str = ""):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "account": account
        }
        self.log_history.append(entry)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.broadcast({"type": "log", "data": entry}),
                    loop
                )
        except Exception:
            pass


HUB = BroadcastHub()


def log(msg):
    print(msg, flush=True)
    HUB.log_sync("info", str(msg))

def print_error(msg):
    print(f"[-] {msg}", flush=True)
    HUB.log_sync("error", str(msg))

def print_success(msg):
    print(f"[+] {msg}", flush=True)
    HUB.log_sync("success", str(msg))

def print_info(msg):
    print(f"[i] {msg}", flush=True)
    HUB.log_sync("info", str(msg))

def print_warning(msg):
    print(f"[!] {msg}", flush=True)
    HUB.log_sync("warning", str(msg))


# ============================================================
#  HTTP CLIENT
# ============================================================

client = httpx.AsyncClient(
    verify=False,
    timeout=15.0,
    limits=httpx.Limits(max_connections=300, max_keepalive_connections=150)
)
_LOGIN_SEMAPHORE = asyncio.Semaphore(4)

headers = {
    'User-Agent': 'UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Expect': '100-continue',
    'X-Unity-Version': '2018.4.12f1',
    'X-GA-SV': '1789535859',
    'X-GA': 'v1 1',
    'ReleaseVersion': 'OB55'
}


# ============================================================
#  HELPERS
# ============================================================

def get_proto_field(d, key, default=None):
    if not d or not isinstance(d, dict):
        return default
    if key in d:
        val = d[key].get('data')
        return val if val is not None else default
    if str(key) in d:
        val = d[str(key)].get('data')
        return val if val is not None else default
    return default


def _pb_varint(n):
    if n < 0:
        n = (1 << 64) + n
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


def _pb_tag(f, w):
    return _pb_varint((f << 3) | w)


def _pb_field(f, v):
    if isinstance(v, bool):
        v = int(v)
    if isinstance(v, int):
        return _pb_tag(f, 0) + _pb_varint(v)
    if isinstance(v, str):
        d = v.encode('utf-8')
        return _pb_tag(f, 2) + _pb_varint(len(d)) + d
    if isinstance(v, (bytes, bytearray)):
        d = bytes(v)
        return _pb_tag(f, 2) + _pb_varint(len(d)) + d
    return b""


def crc7(data):
    c = 0
    for b in data:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F


def has_ssan_zig(n):
    z = (n << 1) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z)
    return bytes(out)


def uleb_encode(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


def _generate_new_device() -> dict:
    device_list = [
        ("Samsung", "SM-G998B", "Adreno (TM) 660", "Android OS 12 / API-31"),
        ("Xiaomi", "2201122G", "Adreno (TM) 730", "Android OS 13 / API-33"),
        ("Realme", "RMX3700", "Mali-G710", "Android OS 14 / API-34"),
        ("OnePlus", "CPH2451", "Adreno (TM) 740", "Android OS 13 / API-33"),
        ("OPPO", "CPH2611", "Adreno (TM) 720", "Android OS 14 / API-34"),
        ("Vivo", "V2203", "Mali-G710", "Android OS 12 / API-31"),
        ("Poco", "M2102J20SG", "Adreno (TM) 660", "Android OS 13 / API-33"),
    ]
    brand, model, gpu, os_ver = random.choice(device_list)
    return {
        "unique_device_id": f"Google|{str(uuid.uuid4())}",
        "brand": brand, "model": model, "gpu_renderer": gpu, "system_software": os_ver,
        "screen_width": random.choice([1080, 1440, 720, 1280]),
        "screen_height": random.choice([2400, 3200, 1600, 2400]),
        "screen_dpi": str(random.randint(300, 420)),
        "memory": random.randint(2800, 6500),
        "processor_details": f"ARM64 FP ASIMD AES VMH | {random.randint(2200, 3200)} | {random.randint(6, 12)}",
        "client_ip": f"{random.randint(103, 223)}.{random.randint(10, 250)}.{random.randint(10, 250)}.{random.randint(10, 250)}"
    }


def get_device_for_account(account_identifier: str) -> dict:
    return _generate_new_device()


async def resolve_host_cloudflare(hostname: str) -> str:
    if not hostname:
        return hostname
    parts = hostname.split('.')
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return hostname
    now = time.time()
    if hostname in _DNS_CACHE:
        ip, exp = _DNS_CACHE[hostname]
        if now < exp:
            return ip

    def _query_cloudflare(server_ip: str) -> Optional[str]:
        s = None
        try:
            tx_id = random.randint(1000, 65535)
            header = struct.pack(">HHHHHH", tx_id, 0x0100, 1, 0, 0, 0)
            qname = b"".join(bytes([len(part)]) + part.encode('ascii') for part in hostname.split('.')) + b"\x00"
            query_pkt = header + qname + struct.pack(">HH", 1, 1)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(1.2)
            s.sendto(query_pkt, (server_ip, 53))
            resp, _ = s.recvfrom(1024)
            if len(resp) >= 12:
                ancount = struct.unpack(">H", resp[6:8])[0]
                if ancount > 0:
                    offset = 12 + len(qname) + 4
                    for _ in range(ancount):
                        if offset >= len(resp):
                            break
                        if (resp[offset] & 0xC0) == 0xC0:
                            offset += 2
                        else:
                            while offset < len(resp) and resp[offset] != 0:
                                offset += 1 + resp[offset]
                            offset += 1
                        if offset + 10 > len(resp):
                            break
                        rtype, rclass, ttl, rdlen = struct.unpack(">HHIH", resp[offset:offset+10])
                        offset += 10
                        if rtype == 1 and rdlen == 4 and offset + 4 <= len(resp):
                            return socket.inet_ntoa(resp[offset:offset+4])
                        offset += rdlen
        except Exception:
            pass
        finally:
            if s:
                try: s.close()
                except Exception: pass
        return None

    loop = asyncio.get_running_loop()
    ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_PRIMARY_DNS)
    if not ip:
        ip = await loop.run_in_executor(None, _query_cloudflare, CLOUDFLARE_SECONDARY_DNS)
    if not ip:
        try:
            ip_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            if ip_info:
                ip = ip_info[0][4][0]
        except Exception:
            ip = hostname
    if ip:
        _DNS_CACHE[hostname] = (ip, now + _DNS_CACHE_TTL)
    return ip or hostname


def optimize_tcp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "SIO_KEEPALIVE_VALS") and os.name == 'nt':
            try: sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 10000, 2000))
            except Exception: pass
        elif hasattr(socket, "TCP_KEEPIDLE"):
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                if hasattr(socket, "TCP_KEEPINTVL"):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
                if hasattr(socket, "TCP_KEEPCNT"):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            except Exception: pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except Exception:
        pass


def optimize_udp_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
        if hasattr(socket, 'SIO_UDP_CONNRESET') and os.name == 'nt':
            try: sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception: pass
    except Exception:
        pass


async def safe_close_writer(writer):
    if not writer:
        return
    try:
        if not writer.is_closing():
            writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=1.5)
    except Exception:
        pass


async def aes_encrypt(payload, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(payload, AES.block_size))


async def get_playstore_version():
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None, lambda: play_scraper('com.dts.freefireth', lang='hi', country='id')
        )
        return result.get("version")
    except Exception:
        return "1.132.8"


async def version_config():
    app_version = await get_playstore_version()
    api_url = (
        "https://version.ggwhitehawk.com/live/ver.php"
        f"?version={app_version}"
        "&lang=hi&device=android&channel=android"
        "&appstore=googleplay&region=ME"
        "&whitelist_version=1.3.0&whitelist_sp_version=1.0.0"
    )
    try:
        response = await client.get(api_url)
        response.raise_for_status()
        data = response.json()
        server_url = data.get("server_url")
        remote_version = data.get("remote_version")
        latest_release_version = data.get("latest_release_version")
        if not server_url or not remote_version or not latest_release_version:
            return None
        return latest_release_version, remote_version, server_url
    except Exception:
        return None


async def get_access_token(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    hdrs = {
        "Host": "100067.connect.garena.com",
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B Build/SP1A.210812.016)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"
    }
    data = {
        "uid": uid, "password": password,
        "response_type": "token", "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"
    }
    for attempt in range(5):
        try:
            response = await client.post(url, headers=hdrs, data=data)
            if response.status_code == 200:
                rd = response.json()
                open_id = rd.get("open_id")
                access_token = rd.get("access_token")
                platform = rd.get("platform", 4)
                if open_id and access_token:
                    return open_id, access_token, platform
            if response.status_code == 429:
                await asyncio.sleep(1)
                continue
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return None


async def parse_results(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {"wire_type": result.wire_type}
        if result.wire_type in ("varint", "string", "bytes"):
            field_data["data"] = result.data
        elif result.wire_type == "length_delimited":
            if hasattr(result.data, "results"):
                field_data["data"] = await parse_results(result.data.results)
            elif isinstance(result.data, list):
                field_data["data"] = await parse_results(result.data)
            else:
                field_data["data"] = str(result.data)
        result_dict[str(result.field)] = field_data
    return result_dict


async def decode_protobuf(data):
    if not HAS_PROTO_DECODER:
        return "{}"
    try:
        parsed_results = Parser().parse(data)
        parsed_results_dict = await parse_results(parsed_results)
        return json.dumps(parsed_results_dict)
    except Exception:
        return "{}"


# ============================================================
#  LOGIN FLOW
# ============================================================

async def build_majorlogin_payload(open_id, access_token, platform, client_version, device_info, verr=None):
    try:
        if verr is None:
            verr = client_version
        proto = thunderFF_pb2.MajorLoginReq()
        proto.event_time = str(datetime.now())[:-7]
        proto.game_name = "free fire"
        proto.platform_id = 1
        proto.client_version = str(verr)
        proto.client_version_code = "2019121229"
        proto.system_software = "Android OS 15 / API-35 (AP3A.240905.015.A2/185014)"
        proto.system_hardware = "Handheld"
        proto.device_type = "Handheld"
        proto.screen_width = 1600
        proto.screen_height = 719
        proto.screen_dpi = "234"
        proto.processor_details = "ARM64 FP ASIMD AES | 1820 | 8"
        proto.memory = 2798
        proto.gpu_renderer = "Mali-G57"
        proto.gpu_version = "OpenGL ES 3.2 v1.r49p1-04eac0.2848c17a2fd4e9340e06555168eaa3c9"
        proto.unique_device_id = "Google|f744e396-5694-4e65-995d-97a958f2bd1f"
        proto.client_ip = "197.0.137.129"
        proto.language = "pt-br"
        proto.open_id = str(open_id)
        proto.open_id_type = "4"
        proto.login_open_id_type = 4
        proto.access_token = str(access_token)
        proto.login_by = 2
        proto.platform_sdk_id = 1
        proto.origin_platform_type = "4"
        proto.primary_platform_type = "4"
        proto.reg_avatar = 1
        proto.channel_type = 3
        proto.telecom_operator = "TUNTEL"
        proto.network_operator_a = "TUNTEL"
        proto.network_type = "WIFI"
        proto.network_type_a = "WIFI"
        proto.cpu_type = 2
        proto.cpu_architecture = "64"
        proto.graphics_api = "OpenGLES2"
        proto.supported_astc_bitset = 8191
        proto.client_using_version = "7428b253defc164018c604a1ebbfebdf"
        proto.loading_time = 15078
        proto.release_channel = "android"
        proto.extra_info = "KqsHTx3+QOmBRR1WKvaWewlcpqJBfjki+PPHQoQG8+0yV+Uos7gUFFjHMQ/e7u6han6Fl77r7c3vMN3p8UbKgN+nfycQCgwBmWgBzomx2gj84c+p"
        proto.android_engine_init_flag = 111207
        proto.if_push = 1
        proto.is_vpn = 0

        memory_available = proto.memory_available
        memory_available.version = 55
        memory_available.hidden_value = 81

        proto.external_storage_total = 49973
        proto.external_storage_available = 11338
        proto.internal_storage_total = 854
        proto.internal_storage_available = 11466
        proto.game_disk_storage_total = 49973
        proto.game_disk_storage_available = 11466
        proto.external_sdcard_total_storage = 49973
        proto.external_sdcard_avail_storage = 11466

        proto.library_path = "/data/app/~~lHFxTCCbupG2QVJmsURtZw==/com.dts.freefireth-N3aCHpHNXpdxjD80uIIbww==/lib/arm64"
        proto.library_token = "b8e0cd5e295eee42f5860d3c86e483dd|/data/app/~~lHFxTCCbupG2QVJmsURtZw==/com.dts.freefireth-N3aCHpHNXpdxjD80uIIbww==/base.apk"

        base_payload = proto.SerializeToString()
        extra = b""
        extra += _pb_field(96, '{"cur_rate":[90,60,120],"support_etc2":false}')
        extra += _pb_field(97, 1)
        extra += _pb_field(99, "4")
        extra += _pb_field(100, "4")
        extra += _pb_field(102, b"\x17]ENWU\x0eR5")
        extra += _pb_field(104, 52882)
        extra += _pb_field(105, 1)
        extra += _pb_field(106, "https://dl.ak.freefiremobile.com/live/ABHotUpdates/|https://core-ak.freefiremobile.com/live/ABHotUpdates/|6b2078db9d22dd98f8e9386a39af8462")
        extra += _pb_field(107, "c8e41b7a93f02d56e1a94c7b8203f5d1")

        full_payload = base_payload + extra
        return await aes_encrypt(full_payload, AES_KEY, AES_IV)
    except Exception as e:
        print_error(f"build_majorlogin_payload error: {e}")
        return None


async def send_majorlogin(data, release_version, server_url):
    try:
        if not server_url or not isinstance(server_url, str) or not server_url.startswith(("http://", "https://")):
            print_error(f"Invalid server_url from version config: {server_url}")
            return None

        url = f"{server_url}MajorLogin" if server_url.endswith('/') else f"{server_url}/MajorLogin"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = str(release_version)
        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            print_error(f"MajorLogin HTTP {response.status_code}")
            return None
        response_content = response.content
        if len(response_content) < 40:
            print_error(f"MajorLogin response too short: {len(response_content)}")
            return None

        res_proto = thunderFF_pb2.MajorLoginRes()
        try:
            res_proto.ParseFromString(response_content)
        except Exception:
            pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception:
            pass

        key_val = get_proto_field(dict_res, 22)
        iv_val = get_proto_field(dict_res, 23)
        if not key_val:
            key_val = res_proto.aes_ak
        if not iv_val:
            iv_val = res_proto.iv_i

        if isinstance(key_val, str):
            try: key_val = bytes.fromhex(key_val)
            except Exception: pass
        if isinstance(iv_val, str):
            try: iv_val = bytes.fromhex(iv_val)
            except Exception: pass

        if key_val:
            try: res_proto.aes_ak = key_val
            except Exception: pass
        if iv_val:
            try: res_proto.iv_i = iv_val
            except Exception: pass

        # ✅ FIX: URL safety fallback
        try:
            current_url = getattr(res_proto, 'url', None)
            if not current_url or not isinstance(current_url, str) or not current_url.startswith(("http://", "https://")):
                res_proto.url = FALLBACK_GETLOGIN_URL
        except Exception:
            res_proto.url = FALLBACK_GETLOGIN_URL

        return res_proto
    except Exception as e:
        print_error(f"send_majorlogin error: {e}")
        return None


async def send_getlogin(data, base_url, token, release_version):
    try:
        # ✅ FIX: base_url safety check
        if not base_url or not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
            print_warning(f"Invalid GetLogin base_url ({base_url}) — using fallback: {FALLBACK_GETLOGIN_URL}")
            base_url = FALLBACK_GETLOGIN_URL

        url = f"{base_url.rstrip('/')}/GetLoginData"
        req_headers = headers.copy()
        req_headers["ReleaseVersion"] = release_version
        req_headers['Authorization'] = f"Bearer {token}"
        try:
            from urllib.parse import urlparse
            req_headers['Host'] = urlparse(base_url).netloc or "clientbp.ggpolarbear.com"
        except Exception:
            req_headers['Host'] = "clientbp.ggpolarbear.com"

        response = await client.post(url, headers=req_headers, data=data)
        if response.status_code != 200:
            print_error(f"GetLoginData HTTP {response.status_code}")
            return None
        response_content = response.content

        res_proto = thunderFF_pb2.GetLoginDataRes()
        try:
            res_proto.ParseFromString(response_content)
        except Exception:
            pass

        def _extract_addr(raw_bytes, field_no):
            try:
                pos = 0
                n = len(raw_bytes)
                def read_varint(buf, p):
                    res = 0
                    sh = 0
                    while p < len(buf):
                        b = buf[p]
                        p += 1
                        res |= (b & 0x7F) << sh
                        if not (b & 0x80):
                            break
                        sh += 7
                    return res, p
                fields = {}
                while pos < n:
                    try:
                        key, pos = read_varint(raw_bytes, pos)
                    except Exception:
                        break
                    fn = key >> 3
                    wt = key & 0x07
                    try:
                        if wt == 0:
                            val, pos = read_varint(raw_bytes, pos)
                        elif wt == 2:
                            ln, pos = read_varint(raw_bytes, pos)
                            val = raw_bytes[pos:pos + ln]
                            pos += ln
                        elif wt == 5:
                            val = raw_bytes[pos:pos + 4]
                            pos += 4
                        elif wt == 1:
                            val = raw_bytes[pos:pos + 8]
                            pos += 8
                        else:
                            break
                    except Exception:
                        break
                    fields.setdefault(fn, []).append(val)
                if field_no in fields:
                    v = fields[field_no][0]
                    if isinstance(v, bytes):
                        try:
                            return v.decode('utf-8', 'ignore')
                        except Exception:
                            return None
                    return str(v)
            except Exception:
                return None

        try:
            for offset in range(0, min(80, len(response_content))):
                fa = _extract_addr(response_content[offset:], 14)
                ia = _extract_addr(response_content[offset:], 32)
                if fa and ":" in fa and ia and ":" in ia:
                    res_proto.functional_addrs = fa
                    res_proto.informational_addrs = ia
                    break
        except Exception:
            pass

        dict_res = {}
        try:
            parsed = Parser().parse(response_content.hex())
            dict_res = await parse_results(parsed)
        except Exception:
            pass

        return res_proto, dict_res
    except Exception as e:
        print_error(f"send_getlogin error: {e}")
        return None


# ============================================================
#  TCP PACKETS
# ============================================================

async def build_tcp_startup_packet(account_id, token, server_time, key, iv, region="ME", typ='OnLine'):
    uid_hex = f"{int(account_id):016x}"
    timestamp_hex = f"{int(server_time):08x}"
    encode_token = token.encode()
    encrypted_packet = AES.new(key, AES.MODE_CBC, iv).encrypt(pad(encode_token, 16)).hex()
    encrypted_packet_length = f"{len(encrypted_packet) // 2:08x}"
    if typ == 'OnLine':
        return f"9015{uid_hex}{timestamp_hex}00000000{encrypted_packet_length}{encrypted_packet}"
    else:
        return f"9015{uid_hex}{timestamp_hex}{encrypted_packet_length}{encrypted_packet}"


async def send_keep_alive(region="ME"):
    try:
        reg = str(region).upper() if region else "ME"
        ka_hex = "0219" if reg == "ME" else ("0214" if reg == "IND" else "0215")
        return bytes.fromhex(ka_hex)
    except Exception:
        return bytes.fromhex("0219")


async def start_game_battle_royale(region, client_version, writer, key, iv):
    packet = bytes.fromhex("080112800a0a010110013a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f504540014a0801090a0b1219202758016291090a8001303838463832424630324139363736373032303130313030303030303030303030303136303030313030313530303032323246393745454530463030303030303436373632353134303030303030303030303030303030303030303030303030303030303030303030303030303066663030303030303030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e75196001477c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b625f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d615f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e5078487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b441b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f477776484f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b3247525268377a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a565072564b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c4434744478706771385533595973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e4249725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d74757077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a201050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815")
    proto = thunderFF_pb2.StartMatch()
    proto.ParseFromString(packet)
    if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
        proto.main.region_list[0].region = region
        if len(proto.main.region_list) > 1:
            proto.main.region_list[1].region = region
    if hasattr(proto.main, 'client_version'):
        proto.main.client_version.remote_version = client_version
    packet = proto.SerializeToString()
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "ME"
    reg_prefix = "031900" if reg == "ME" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()
    log(f"[BR] Match search sent ({packet_length} bytes) | Region: {reg}")


# ============================================================
#  TEA CBC
# ============================================================

async def tea_enc(v0, v1, k0, k1, k2, k3):
    s = 0
    for _ in range(_ROUNDS):
        s = (s + _DELTA) & 0xFFFFFFFF
        v0 = (v0 + (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v1 = (v1 + (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
    return v0, v1


async def tea_dec(v0, v1, k0, k1, k2, k3):
    s = (_DELTA * _ROUNDS) & 0xFFFFFFFF
    for _ in range(_ROUNDS):
        v1 = (v1 - (((((v0 << 4) & 0xFFFFFFFF) + k2) & 0xFFFFFFFF ^
                      ((v0 + s) & 0xFFFFFFFF) ^
                      (((v0 >> 5) + k3) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        v0 = (v0 - (((((v1 << 4) & 0xFFFFFFFF) + k0) & 0xFFFFFFFF ^
                      ((v1 + s) & 0xFFFFFFFF) ^
                      (((v1 >> 5) + k1) & 0xFFFFFFFF)))) & 0xFFFFFFFF
        s = (s - _DELTA) & 0xFFFFFFFF
    return v0, v1


async def tea_cbc_encrypt(padded, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(padded))
    prev_cipher = bytearray(8)
    prev_intermediate = bytearray(8)
    for i in range(0, len(padded), 8):
        xored = bytearray(8)
        for j in range(8):
            xored[j] = padded[i + j] ^ prev_cipher[j]
        e0, e1 = await tea_enc(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3,
        )
        enc = bytearray(8)
        struct.pack_into("<I", enc, 0, e0)
        struct.pack_into("<I", enc, 4, e1)
        for j in range(8):
            out[i + j] = enc[j] ^ prev_intermediate[j]
        prev_cipher[:] = out[i:i + 8]
        prev_intermediate[:] = xored
    return bytes(out)


async def tea_cbc_decrypt(body, key_bytes):
    k0, k1, k2, k3 = (struct.unpack_from("<I", key_bytes, o)[0] for o in (0, 4, 8, 12))
    out = bytearray(len(body))
    prev_intermediate = bytearray(8)
    prev_cipher = bytearray(8)
    xored = bytearray(8)
    dec = bytearray(8)
    for i in range(0, len(body), 8):
        for j in range(8):
            xored[j] = body[i + j] ^ prev_intermediate[j]
        d0, d1 = await tea_dec(
            struct.unpack_from("<I", xored, 0)[0],
            struct.unpack_from("<I", xored, 4)[0],
            k0, k1, k2, k3
        )
        struct.pack_into("<I", dec, 0, d0)
        struct.pack_into("<I", dec, 4, d1)
        for j in range(8):
            out[i + j] = dec[j] ^ prev_cipher[j]
        prev_cipher[:] = body[i:i + 8]
        prev_intermediate[:] = dec
    return bytes(out)


async def build_padded(content):
    pad_len = (8 - (len(content) + 10) % 8) % 8
    return bytes([pad_len, 0, 0]) + b"\x00" * pad_len + content + b"\x00" * 7


async def encode_header(layout, send_option, cmd, order_id, flags, length, k, v80):
    out = bytearray()
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: length}[code]
        if _FIELD_SIZES[code] == 1:
            out.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            out.append(v & 0xFF)
            out.append((v >> 8) & 0xFF)
    return bytes(out)


async def crc7_buff(crc, buf):
    c = crc & 0x7F
    for b in buf:
        c = CRC7_TABLE[((2 * (c & 0xFF)) ^ (b & 0xFF)) & 0xFF] & 0x7F
    return c & 0x7F


async def sv_frame(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0]) + await encode_header(layout, send_option, cmd, order_id, flags, len(body), k, v80)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def build_match_startup_packets(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region="ME", client_version="1.132.8",
                                      client_version_code="2019121229", access_token="",
                                      mode_id=MODE_ID, map_id=MAP_ID):
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]

    thunder_jwt = token[:660] if len(token) > 660 else token
    sharma_jwt = token[660:] if len(token) > 660 else ""
    encoded_thunder_jwt = thunder_jwt.encode() if isinstance(thunder_jwt, str) else thunder_jwt
    encoded_sharma_jwt = sharma_jwt.encode() if isinstance(sharma_jwt, str) else sharma_jwt

    garena420 = has_ssan_zig(len(encoded_thunder_jwt)) + encoded_thunder_jwt
    reg = str(region).upper() if region else "ME"

    csoversea_block = bytes.fromhex(
        "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
        "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
        "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
        "00000800000100000000000100a8a2d7bebd8d8bdf110200"
    )

    mid = bytes.fromhex('0000000001000102030101') + has_ssan_zig(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += has_ssan_zig(len(client_version)) + client_version.encode()
    mid += has_ssan_zig(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block

    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += has_ssan_zig(len(clean_ip)) + clean_ip.encode()

    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += has_ssan_zig(len(clean_acc_tok)) + clean_acc_tok.encode()

    mid += has_ssan_zig(len(encoded_sharma_jwt)) + encoded_sharma_jwt

    tg_garena420 = (
        uleb_encode(int(account_id)) +
        uleb_encode(int(block_val)) +
        uleb_encode(1) +
        uleb_encode(int(mode_id)) +
        uleb_encode(int(block_val)) +
        uleb_encode(int(map_id)) +
        mid
    )

    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, garena420, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, tg_garena420, udp_key)
    return process.hex(), loading.hex()


async def produce_xor_key(secret_key):
    k = secret_key[0] if secret_key and len(secret_key) > 0 else 10
    return k, ((k << 8) | k) & 0xFFFF


async def parse_layout(layout):
    if isinstance(layout, str):
        return [int(ch) for ch in layout.strip()]
    return list(layout)


async def build_hello_packet(text, key, layout):
    data = text.encode("utf-8")
    if len(data) > 25:
        raise ValueError(f"Text is too long ({len(data)} bytes)")
    content = b"\x10\x00\x00\x00" + data + b"\x00" * (29 - 4 - len(data))
    k, v80 = await produce_xor_key(key)
    layout = await parse_layout(layout)
    padded = await build_padded(content)
    enc_body = await tea_cbc_encrypt(padded, key)
    header_bytes = await encode_header(layout, 1, 1, 0, 1, len(enc_body), k, v80)
    packet = bytearray([0x63, 0x00]) + header_bytes + enc_body
    packet[1] = await crc7_buff(0, packet[2:]) & 0x7F
    return bytes(packet).hex()


async def classify(frame):
    cmd = frame["cmd"]
    msg_name = MESSAGE_ID_TO_NAME.get(cmd, f"UNKNOWN_{cmd}")
    if msg_name == "UDP_HELLO":
        return "HELLO"
    if msg_name == "UDP_ACK":
        return "ACK"
    if msg_name == "UDP_PING":
        return "PING"
    if msg_name == "RUDP_JOIN_MATCH":
        return "JOIN_MATCH"
    if msg_name.startswith("RUDP_"):
        return msg_name
    if msg_name.startswith("UDP_"):
        return msg_name
    return "DATA"


async def build_packet(msg_key, layout, send_option, cmd, order_id, flags, content, key, encrypted=True):
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    body = await tea_cbc_encrypt(await build_padded(content), key) if encrypted else content
    hdr = bytearray([msg_key, 0])
    for code in layout:
        value = {0: send_option, 1: cmd, 2: order_id, 3: flags, 4: len(body)}[code]
        if _FIELD_SIZES[code] == 1:
            hdr.append((value & 0xFF) ^ k)
        else:
            v = ((value & 0xFFFF) ^ v80) & 0xFFFF
            hdr.append(v & 0xFF)
            hdr.append((v >> 8) & 0xFF)
    packet = bytearray(hdr + body)
    packet[1] = await crc7_buff(0, bytes(packet[2:])) & 0x7F
    return bytes(packet)


async def layouts_from_mask(mask):
    ru = [int(c) for c in str(mask).strip()]
    nr = [c for c in ru if c != 2]
    return ru, nr


async def reply_for(frame, key, mask, ack_key=0x68, ping_key=0x6D, hello_key=0x5B, ack_style="short"):
    ru, nr = await layouts_from_mask(mask)
    typ = await classify(frame)
    if typ == "HELLO":
        if ack_style == "echo":
            content = frame["content"] if frame["content"] else b"\x10\x00\x00\x00"
            return typ, await build_packet(hello_key, nr, 1, 1, None, 1, content, key)
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x01\x00", key)
    if typ == "ACK":
        content = frame["content"] if frame["content"] else b"\x01\x00"
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, content, key)
    if typ == "PING":
        c = frame["content"]
        counter = c[:4] if len(c) >= 4 else c
        return typ, await build_packet(ping_key, nr, 0, 3, None, 0, counter + b"\x00\x00\x00", key, encrypted=False)
    if typ == "JOIN_MATCH":
        return typ, await build_packet(ack_key, nr, 0, 2, None, 1, b"\x02\x00", key)
    return typ, None


async def keepalive_ping(sock, ip, port, key_bytes, mask, stop_event):
    nr = (await layouts_from_mask(mask))[1]
    ping_keys = [0x66, 0x6D, 0x69, 0x6C, 0x6B, 0x6E, 0x6F, 0x70]
    loop = asyncio.get_event_loop()
    i = 0
    while not stop_event.is_set():
        pk = ping_keys[i % len(ping_keys)]
        counter = int(time.time() * 1000) & 0xFFFFFFFF
        pkt = await build_packet(pk, nr, 0, 3, None, 0, struct.pack("<I", counter) + b"\x00\x00\x00", key_bytes, encrypted=False)
        try:
            await loop.sock_sendto(sock, pkt, (ip, port))
        except Exception:
            pass
        i += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass


async def try_header(buf, layout, k, v80):
    off = 2
    out = {}
    for code in layout:
        size = _FIELD_SIZES[code]
        if off + size > len(buf):
            return None
        out[_FIELD_NAMES[code]] = (buf[off] ^ k) if size == 1 else ((buf[off] | (buf[off + 1] << 8)) ^ v80) & 0xFFFF
        off += size
    out["headerLen"] = off
    return out


async def oicq_unpad(padded):
    if not padded or len(padded) < 8:
        return None
    if not all(padded[-1 - i] == 0 for i in range(7)):
        return None
    pad_len = padded[0] & 0x07
    s = 3 + pad_len
    e = len(padded) - 7
    return padded[s:e] if s < e else b""


async def decode_packet(packet, key, mask=None):
    data = bytes(packet) if isinstance(packet, bytes) else bytes.fromhex(packet)
    if len(data) < 8:
        return None
    k = key[0]
    v80 = ((k << 8) | k) & 0xFFFF
    crc_ok = (data[1] & 0x7F) == await crc7_buff(0, data[2:])
    candidates = []
    if mask:
        ru, nr = await layouts_from_mask(mask)
        layouts = [("RUDP", ru), ("nonRUDP", nr)]
    else:
        layouts = [("RUDP", list(p)) for p in itertools.permutations([0, 1, 2, 3, 4])]
        layouts += [("nonRUDP", list(p)) for p in itertools.permutations([0, 1, 3, 4])]
    for kind, layout in layouts:
        f = await try_header(data, layout, k, v80)
        if not f:
            continue
        if f["flags"] > 7 or f["sendOption"] > 7:
            continue
        if f["length"] != len(data) - f["headerLen"]:
            continue
        body = data[f["headerLen"]:f["headerLen"] + f["length"]]
        content = None
        padded = None
        if f["flags"] & 1:
            if len(body) < 8 or len(body) % 8 != 0:
                continue
            padded = await tea_cbc_decrypt(body, key)
            content = await oicq_unpad(padded)
            if content is None:
                continue
        else:
            content = body
        score = (1 if crc_ok else 0) + (1 if content is not None else 0)
        candidates.append({
            "kind": kind, "layout": layout, "headerLen": f["headerLen"],
            "msgKey": data[0], "cmd": f["cmd"], "flags": f["flags"],
            "sendOption": f["sendOption"], "orderId": f.get("orderId"),
            "length": f["length"], "content": content, "crcOk": crc_ok,
            "padded": padded, "score": score, "total": len(data),
        })
    if not candidates:
        return None
    candidates.sort(key=lambda c: (c["kind"] == "RUDP" or c["kind"] == "nonRUDP", c["score"]), reverse=True)
    return candidates[0]


# ============================================================
#  GAME PLAYER
# ============================================================

async def play_game(server_ip_port, thunder, sharma, udp_key, match_code,
                    account_id, player_region, client_version, key, iv,
                    match_index: int):
    match_start_time = time.time()
    ping_task = None
    sock = None
    ping_stop = asyncio.Event()
    uid_str = str(account_id)
    completed_cleanly = False

    HUB.stats["active_matches"] += 1
    try:
        await HUB.update_stats()
    except Exception:
        pass

    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)

        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(('0.0.0.0', 0))
        except Exception:
            pass
        optimize_udp_socket(sock)
        sock.setblocking(False)

        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
        try:
            await loop.sock_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))
            log(f"[MATCH #{match_index}] ✅ HELLO sent ({len(bytes.fromhex(hello_packet))}B) -> {resolved_ip}:{port}")
        except Exception as e:
            log(f"[MATCH #{match_index}] ❌ HELLO failed: {e}")

        ack_state = "waiting_for_hello_reply"
        thunder_sent = False
        sharma_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()

        ping_task = asyncio.create_task(
            keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop)
        )

        last_activity = time.time()
        MAX_IDLE_BEFORE_HELLO_RESEND = 7.0

        async def send_thunder_sharma_inline():
            nonlocal ack_state, thunder_sent, sharma_sent
            if thunder_sent:
                return
            async with send_lock:
                if thunder_sent:
                    return
                try:
                    thunder_b = bytes.fromhex(thunder)
                    await loop.sock_sendto(sock, thunder_b, (resolved_ip, port))
                    log(f"[MATCH #{match_index}] ⚡ THUNDER sent ({len(thunder_b)}B)")
                    thunder_sent = True

                    await asyncio.sleep(0.3)

                    prepare_ack = await build_packet(
                        0x68, (await layouts_from_mask(match_code))[1],
                        0, 2, None, 1, b"\x01\x00", udp_key_bytes
                    )
                    await loop.sock_sendto(sock, prepare_ack, (resolved_ip, port))
                    log(f"[MATCH #{match_index}] 📤 PREPARE_ACK sent")

                    await asyncio.sleep(0.4)

                    sharma_b = bytes.fromhex(sharma)
                    await loop.sock_sendto(sock, sharma_b, (resolved_ip, port))
                    log(f"[MATCH #{match_index}] 🌀 SHARMA sent ({len(sharma_b)}B)")

                    sharma_sent = True
                    ack_state = "thunder_sharma_sent"
                    log(f"[MATCH #{match_index}] 🎮 Startup delivered. Playing.")
                except Exception as e:
                    log(f"[MATCH #{match_index}] ❌ Startup failed: {e}")

        while not local_closed:
            elapsed = time.time() - match_start_time
            if elapsed > MAX_MATCH_DURATION:
                completed_cleanly = True
                break

            try:
                response, server_addr = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 65535), timeout=2.0
                )
                if response:
                    last_activity = time.time()
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if frame:
                        ptype = await classify(frame)
                        cmd = frame['cmd']

                        if cmd in [103, 107]:
                            log(f"[MATCH #{match_index}] 🏁 Match finished (cmd {cmd})")
                            completed_cleanly = True
                            local_closed = True
                            continue

                        if cmd == 101:
                            try:
                                ack_pkt = await build_packet(
                                    0x68, (await layouts_from_mask(match_code))[1],
                                    0, 2, None, 1, b"\x01\x00", udp_key_bytes
                                )
                                await loop.sock_sendto(sock, ack_pkt, server_addr)
                            except Exception:
                                pass

                        if ptype in ["ACK", "PING", "HELLO", "JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                typ, reply = await reply_for(
                                    frame, udp_key_bytes, match_code, ack_style="short"
                                )
                                if reply:
                                    try:
                                        await loop.sock_sendto(sock, reply, server_addr)
                                    except Exception:
                                        pass
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state == "waiting_for_hello_reply":
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        try:
                                            await loop.sock_sendto(sock, reply, server_addr)
                                        except Exception:
                                            pass
                                    ack_state = "ready_to_send_thunder"
                                elif ack_state == "ack_sent_waiting":
                                    ack_state = "ready_to_send_thunder"
                                else:
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        try:
                                            await loop.sock_sendto(sock, reply, server_addr)
                                        except Exception:
                                            pass
                            elif ptype == "PING":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    try:
                                        await loop.sock_sendto(sock, reply, server_addr)
                                    except Exception:
                                        pass
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    try:
                                        await loop.sock_sendto(sock, reply, server_addr)
                                    except Exception:
                                        pass
                                    join_match_received = True

            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_thunder" and not thunder_sent:
                    await send_thunder_sharma_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > MAX_IDLE_BEFORE_HELLO_RESEND:
                        try:
                            pkt = await build_hello_packet(
                                f"{account_id}_2585", udp_key_bytes, match_code
                            )
                            await loop.sock_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception:
                            pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 25.0:
                        break

            except BlockingIOError:
                await asyncio.sleep(0.05)
            except OSError:
                await asyncio.sleep(0.5)
                continue
            except Exception:
                await asyncio.sleep(0.5)
                continue

            if ack_state == "ready_to_send_thunder" and not thunder_sent:
                await send_thunder_sharma_inline()

        if completed_cleanly:
            HUB.stats["total_matches"] += 1
            HUB.stats["session_matches"] += 1

        return f"match #{match_index} finished"

    except Exception as e:
        log(f"[MATCH #{match_index}] Session error: {e}")
        return f"match #{match_index} error"

    finally:
        ping_stop.set()
        if ping_task:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        HUB.stats["active_matches"] = max(0, HUB.stats["active_matches"] - 1)
        try:
            await HUB.update_stats()
        except Exception:
            pass


async def functional_br(addrs, starter_packet, account_region, client_version,
                        key, iv, account_id="", account_data=None,
                        max_reconnects=10):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    search_attempts = 0
    last_start_time = 0.0
    uid_str = str(account_id)

    current_token = starter_packet
    current_key = key
    current_iv = iv
    current_account_data = account_data

    try:
        while True:
            writer = None
            gateway_ping_task = None
            try:
                resolved_ip = await resolve_host_cloudflare(ip)
                reader, writer = await asyncio.open_connection(resolved_ip, int(port))

                raw_sock = writer.get_extra_info('socket')
                if raw_sock:
                    optimize_tcp_socket(raw_sock)

                writer.write(bytes.fromhex(current_token))
                await writer.drain()

                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and writer and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception:
                    pass

                async def func_gateway_keepalive():
                    ka_bytes = await send_keep_alive(account_region)
                    while True:
                        await asyncio.sleep(5)
                        try:
                            if writer and not writer.is_closing():
                                writer.write(ka_bytes)
                                await writer.drain()
                        except Exception:
                            break

                gateway_ping_task = asyncio.create_task(func_gateway_keepalive())

                log(f"[+] TCP gateway connected | UID: {uid_str}")
                reconnects = 0
                no_response_count = 0
                last_start_time = 0.0

                async def send_start_match():
                    nonlocal search_attempts, last_start_time
                    search_attempts += 1
                    current_region = "ME"
                    try:
                        await asyncio.sleep(random.uniform(0.2, 0.4))
                        log(f"[BR] UID {uid_str} -> BR match #{search_attempts}")
                        await start_game_battle_royale(
                            current_region, client_version, writer,
                            current_key, current_iv
                        )
                    except Exception as e:
                        log(f"[!] StartMatch notice: {e}")
                    last_start_time = asyncio.get_running_loop().time()

                await send_start_match()

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]

                    now = asyncio.get_running_loop().time()
                    if len(play_matches) == 0 and (now - last_start_time >= START_MATCH_INTERVAL):
                        await send_start_match()

                    try:
                        data = await asyncio.wait_for(reader.read(65535), timeout=0.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1
                        if no_response_count > 60:
                            no_response_count = 0
                        continue

                    if not data:
                        raise ConnectionError("Connection closed by server")

                    hex_data = data.hex()
                    packet_length = len(data)
                    no_response_count = 0

                    if packet_length < 10:
                        continue

                    is_match_packet = False
                    payload_hex = None

                    if hex_data.startswith("0300"):
                        if 10 < packet_length < 30:
                            log(f"[*] Queue confirmed (BR) | UID: {uid_str}")
                            continue
                        elif packet_length >= 300:
                            is_match_packet = True
                            payload_hex = hex_data[10:]
                        else:
                            continue
                    elif packet_length >= 200:
                        is_match_packet = True
                        payload_hex = hex_data

                    if not is_match_packet:
                        continue

                    log(f"[+] 🎯 Match found [BR] | UID: {uid_str} | size={packet_length}")

                    try:
                        res = json.loads(await decode_protobuf(payload_hex))
                        token = None
                        udp_key = None
                        match_code = None
                        server_ip_port = None
                        match_account_id = None
                        block_val = None

                        if '42' in res and 'data' in res['42']:
                            match_code = res['42']['data']
                        if '5' in res and 'data' in res['5']:
                            res_field5 = res['5']['data']
                            server_ip_port = res_field5.get('2', {}).get('data')
                            udp_key = res_field5.get('3', {}).get('data')
                            token = res_field5.get('4', {}).get('data')
                            if '42' in res_field5:
                                match_code = res_field5['42']['data']
                        if '1' in res and 'data' in res['1']:
                            match_account_id = res['1']['data']
                        if '5' in res and 'data' in res['5']:
                            block_val = res['5']['data'].get('1', {}).get('data')

                        effective_acc_id = match_account_id or account_id or "ME_BOT"

                        if token and udp_key and match_code and server_ip_port:
                            acc_tok = current_account_data.get('access_token', '') if current_account_data else ""

                            thunder, sharma = await build_match_startup_packets(
                                token, udp_key, match_code, effective_acc_id, block_val or 0,
                                server_ip=server_ip_port,
                                region=account_region,
                                client_version=client_version,
                                access_token=acc_tok,
                                mode_id=MODE_ID,
                                map_id=MAP_ID
                            )

                            match_index = len(play_matches) + 1
                            log(f"[+] Match #{match_index} [BR] injected -> {server_ip_port}")

                            new_match = asyncio.create_task(
                                play_game(
                                    server_ip_port,
                                    thunder,
                                    sharma,
                                    udp_key,
                                    match_code,
                                    effective_acc_id,
                                    "ME",
                                    client_version,
                                    current_key,
                                    current_iv,
                                    match_index=match_index
                                )
                            )
                            play_matches.append(new_match)

                            async def drain_gateway_reader():
                                while not new_match.done():
                                    try:
                                        data_gw = await asyncio.wait_for(reader.read(4096), timeout=1.0)
                                        if not data_gw:
                                            break
                                    except asyncio.TimeoutError:
                                        continue
                                    except Exception:
                                        break

                            drain_task = asyncio.create_task(drain_gateway_reader())

                            try:
                                await new_match
                            except Exception as e:
                                log(f"[MATCH #{match_index}] error: {e}")
                            finally:
                                drain_task.cancel()
                                try:
                                    await drain_task
                                except asyncio.CancelledError:
                                    pass

                            play_matches[:] = [m for m in play_matches if not m.done()]

                            try:
                                await refresh_account_profile(current_account_data)
                            except Exception:
                                pass

                            new_level = 1
                            if current_account_data:
                                new_level = max(1, int(current_account_data.get("level", 1) or 1))
                                HUB.stats["accounts"][str(effective_acc_id)] = {
                                    "nickname": current_account_data.get('nickname', 'Unknown'),
                                    "level": new_level,
                                    "exp": current_account_data.get('exp', 0),
                                    "region": account_region,
                                    "status": "running",
                                    "matches": HUB.stats["accounts"].get(str(effective_acc_id), {}).get("matches", 0) + 1
                                }
                                await HUB.update_stats()

                            log(f"[*] UID {uid_str} | Lvl {new_level} | Next: BR")

                            if gateway_ping_task:
                                gateway_ping_task.cancel()
                            await safe_close_writer(writer)
                            writer = None
                            break

                        else:
                            continue

                    except Exception as e:
                        log(f"[!] Match packet notice: {e}")
                        continue

            except asyncio.CancelledError:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                raise
            except Exception as e:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                play_matches[:] = [m for m in play_matches if not m.done()]
                await safe_close_writer(writer)

                if "Cache expired" in str(e):
                    break

                reconnects += 1
                if reconnects > max_reconnects:
                    reconnects = 0
                    break

                await asyncio.sleep(min(reconnects * 0.5, 2.0))
            finally:
                if gateway_ping_task:
                    gateway_ping_task.cancel()
                if writer:
                    await safe_close_writer(writer)

    except asyncio.CancelledError:
        raise
    finally:
        for m in play_matches:
            if not m.done():
                m.cancel()


async def informational(addrs, starter_packet, key, iv, region="ME", account_id="", max_reconnects=3):
    uid_str = str(account_id)
    reconnects = 0
    ip, port = addrs.split(":")
    while True:
        writer = None
        ping_task = None
        try:
            resolved_ip = await resolve_host_cloudflare(ip)
            reader, writer = await asyncio.open_connection(resolved_ip, int(port))

            raw_sock = writer.get_extra_info('socket')
            if raw_sock:
                optimize_tcp_socket(raw_sock)

            writer.write(bytes.fromhex(starter_packet))
            await writer.drain()
            reconnects = 0

            try:
                init_ka = await send_keep_alive(region)
                if init_ka and writer and not writer.is_closing():
                    writer.write(init_ka)
                    await asyncio.wait_for(writer.drain(), timeout=3)
            except Exception:
                pass

            async def info_keepalive():
                ka_bytes = await send_keep_alive(region)
                while True:
                    await asyncio.sleep(5)
                    try:
                        if writer and not writer.is_closing():
                            writer.write(ka_bytes)
                            await writer.drain()
                    except Exception:
                        break

            ping_task = asyncio.create_task(info_keepalive())

            while True:
                try:
                    data = await asyncio.wait_for(reader.read(8192), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if not data:
                    raise ConnectionError("Connection closed")
        except asyncio.CancelledError:
            if ping_task:
                ping_task.cancel()
            await safe_close_writer(writer)
            raise
        except Exception:
            if ping_task:
                ping_task.cancel()
            await safe_close_writer(writer)
            reconnects += 1
            if reconnects > max_reconnects:
                await asyncio.sleep(3)
                reconnects = 0
            else:
                await asyncio.sleep(1)


async def refresh_account_profile(account_data: Dict):
    try:
        if not account_data:
            return
        url = account_data.get('server_url')
        token = account_data.get('token')
        release_version = account_data.get('release_version')
        payload = account_data.get('login_payload_data')
        if not (url and token and release_version and payload):
            return
        res = await send_getlogin(payload, url, token, release_version)
        if not res:
            return
        res_proto, dict_res = res
        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, "")
        if level <= 0:
            level = 1
        if exp < 0:
            exp = 0
        account_data['level'] = level
        account_data['exp'] = exp
        if nickname:
            account_data['nickname'] = nickname
    except Exception:
        pass


async def process_account_uid_pass(uid: str, password: str) -> Optional[Dict]:
    log(f"[*] Logging in UID: {uid}")
    try:
        async with _LOGIN_SEMAPHORE:
            verconfig_res = await version_config()
            if verconfig_res is None:
                print_error(f"Version config failed for {uid}")
                return None
            release_version, client_version, server_url = verconfig_res

            tokengrant_response = await get_access_token(uid, password)
            if tokengrant_response is None:
                print_error(f"OAuth failed for UID {uid}")
                return None
            open_id, access_token, platform = tokengrant_response

            device_info = get_device_for_account(uid)
            login_payload_data = await build_majorlogin_payload(open_id, access_token, platform, client_version, device_info)
            if login_payload_data is None:
                return None

            majorlogin_response = await send_majorlogin(login_payload_data, release_version, server_url)
            if majorlogin_response is None:
                print_error(f"MajorLogin failed for {uid}")
                return None

            getlogin_result = await send_getlogin(
                login_payload_data,
                majorlogin_response.url,
                majorlogin_response.token,
                release_version
            )
            if getlogin_result is None:
                print_error(f"GetLoginData failed for {uid}")
                return None

            res_proto, dict_res = getlogin_result

        acc_id = str(majorlogin_response.account_id)
        if acc_id == "0" or not acc_id or acc_id == "None":
            print_error(f"Invalid account_id (0) for UID {uid}")
            return None

        level = int(get_proto_field(dict_res, 6, 1))
        exp = int(get_proto_field(dict_res, 7, 0))
        nickname = res_proto.nickname or get_proto_field(dict_res, 4, f"Player_{acc_id}")
        region = majorlogin_response.region or get_proto_field(dict_res, 3, "ME")

        if level <= 0:
            level = 1
        if exp < 0:
            exp = 0

        func_addr = res_proto.functional_addrs
        info_addr = res_proto.informational_addrs
        if not isinstance(func_addr, str) or not func_addr or ":" not in func_addr:
            func_addr = None
        if not isinstance(info_addr, str) or not info_addr or ":" not in info_addr:
            info_addr = None

        key_val = majorlogin_response.aes_ak
        iv_val = majorlogin_response.iv_i
        if not isinstance(key_val, (bytes, bytearray)) or len(key_val) < 16:
            key_val = AES_KEY
        if not isinstance(iv_val, (bytes, bytearray)) or len(iv_val) < 16:
            iv_val = AES_IV

        HUB.stats["total_logins"] += 1
        print_success(f"Login OK | UID {acc_id} | {nickname} | Lvl {level} | Mode: BR")

        account_data = {
            'account_id': majorlogin_response.account_id,
            'nickname': nickname,
            'region': region,
            'level': level,
            'exp': exp,
            'open_id': open_id,
            'access_token': access_token,
            'platform': str(platform),
            'token': majorlogin_response.token,
            'server_time': majorlogin_response.server_time,
            'aes_ak': key_val,
            'iv_i': iv_val,
            'functional_addrs': func_addr,
            'informational_addrs': info_addr,
            'release_version': release_version,
            'client_version': client_version,
            'server_url': majorlogin_response.url,
            'login_payload_data': login_payload_data,
            'auth_type': 'guest',
            'auth_uid': uid,
            'auth_password': password
        }
        return account_data
    except Exception as e:
        print_error(f"process_account_uid_pass error: {e}")
        traceback.print_exc()
        return None


async def run_account_worker(account_data: Dict, label: str):
    acc_id = str(account_data['account_id'])
    informational_task = None
    exp_task = None
    functional_task = None
    try:
        reg = account_data.get('region', 'ME')
        func_addr = account_data.get('functional_addrs')
        info_addr = account_data.get('informational_addrs')

        HUB.stats["accounts"][acc_id] = {
            "nickname": account_data.get('nickname', 'Unknown'),
            "level": account_data.get('level', 1),
            "exp": account_data.get('exp', 0),
            "region": reg,
            "status": "connecting",
            "matches": 0
        }
        await HUB.update_stats()

        if not func_addr or not isinstance(func_addr, str) or ":" not in func_addr:
            print_warning(f"No functional_addrs for {acc_id} — worker idle")
            HUB.stats["accounts"][acc_id]["status"] = "idle"
            await HUB.update_stats()
            while True:
                await asyncio.sleep(60)
            return

        tcp_packet_online = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='OnLine'
        )

        tcp_packet_chat = await build_tcp_startup_packet(
            account_data['account_id'],
            account_data['token'],
            account_data['server_time'],
            account_data['aes_ak'],
            account_data['iv_i'],
            region=reg,
            typ='ChaT'
        )

        HUB.stats["accounts"][acc_id]["status"] = "running"
        await HUB.update_stats()

        if info_addr and isinstance(info_addr, str) and ":" in info_addr:
            informational_task = asyncio.create_task(
                informational(
                    info_addr,
                    tcp_packet_chat,
                    account_data['aes_ak'],
                    account_data['iv_i'],
                    region=reg,
                    account_id=acc_id
                )
            )

        async def exp_refresher():
            while True:
                await asyncio.sleep(120 + random.uniform(-10.0, 10.0))
                try:
                    await refresh_account_profile(account_data)
                    HUB.stats["accounts"][acc_id]["level"] = account_data.get('level', 1)
                    HUB.stats["accounts"][acc_id]["exp"] = account_data.get('exp', 0)
                    await HUB.update_stats()
                except Exception:
                    pass

        exp_task = asyncio.create_task(exp_refresher())

        functional_task = asyncio.create_task(
            functional_br(
                func_addr,
                tcp_packet_online,
                account_data['region'],
                account_data['client_version'],
                account_data['aes_ak'],
                account_data['iv_i'],
                account_id=acc_id,
                account_data=account_data
            )
        )

        await functional_task

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print_error(f"run_account_worker error for {label}: {e}")
        traceback.print_exc()
    finally:
        HUB.stats["accounts"].pop(acc_id, None)
        try:
            await HUB.update_stats()
        except Exception:
            pass
        for t in (informational_task, exp_task, functional_task):
            if t and not t.done():
                t.cancel()
        for t in (informational_task, exp_task, functional_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


async def account_loop_guest(uid: str, password: str):
    uid_str = str(uid)
    while True:
        try:
            log(f"[*] Starting login for UID: {uid_str}")
            account_data = await process_account_uid_pass(uid_str, password)
            if not account_data:
                print_warning(f"Login failed for {uid_str}. Retry in 15s...")
                await asyncio.sleep(15)
                continue
            await run_account_worker(account_data, uid_str)
            log(f"[!] Session ended for {uid_str}. Reconnecting in 3s...")
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            log(f"[!] Worker stopped for {uid_str}")
            break
        except Exception as e:
            print_error(f"Error for UID {uid_str}: {e}")
            await asyncio.sleep(10)


# ============================================================
#  FASTAPI APP
# ============================================================

app = FastAPI(title="FF Level Up Bot", version="3.0.0")


class BotManager:
    def __init__(self):
        self.tasks: List[asyncio.Task] = []
        self.accounts: List[Dict] = []
        self.running = False

    async def start(self, accounts: List[Dict]):
        if self.running:
            return False, "Bot already running"
        self.accounts = accounts
        self.running = True
        HUB.stats["bot_status"] = "running"
        HUB.stats["uptime_start"] = time.time()
        HUB.stats["session_matches"] = 0
        HUB.stats["total_matches"] = 0
        await HUB.log_sync("success", f"🚀 Bot started with {len(accounts)} account(s)")
        for acc in accounts:
            task = asyncio.create_task(account_loop_guest(acc['uid'], acc['password']))
            self.tasks.append(task)
        await HUB.update_stats()
        return True, "Bot started"

    async def stop(self):
        if not self.running:
            return False, "Bot not running"
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self.tasks.clear()
        self.running = False
        HUB.stats["bot_status"] = "stopped"
        HUB.stats["accounts"].clear()
        HUB.stats["active_matches"] = 0
        await HUB.log_sync("warning", "🛑 Bot stopped")
        await HUB.update_stats()
        return True, "Bot stopped"

    def status(self):
        return {
            "running": self.running,
            "accounts": self.accounts,
            "tasks": len(self.tasks)
        }


BOT = BotManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await HUB.connect(ws)
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
                if msg == "ping":
                    await ws.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                try:
                    await ws.send_json({"type": "pong"})
                except Exception:
                    break
            except WebSocketDisconnect:
                break
    except Exception:
        pass
    finally:
        await HUB.disconnect(ws)


@app.get("/api/status")
async def api_status():
    return JSONResponse({
        "bot": BOT.status(),
        "stats": HUB.stats
    })


@app.post("/api/start")
async def api_start(request: Request):
    try:
        body = await request.json()
        accounts = body.get("accounts", [])
        if not accounts:
            return JSONResponse({"ok": False, "message": "No accounts provided"}, status_code=400)
        valid = []
        for acc in accounts:
            if acc.get("uid") and acc.get("password"):
                valid.append({"uid": str(acc["uid"]).strip(), "password": str(acc["password"]).strip()})
        if not valid:
            return JSONResponse({"ok": False, "message": "All accounts invalid"}, status_code=400)
        ok, msg = await BOT.start(valid)
        return JSONResponse({"ok": ok, "message": msg})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@app.post("/api/stop")
async def api_stop():
    ok, msg = await BOT.stop()
    return JSONResponse({"ok": ok, "message": msg})


@app.post("/api/test-connection")
async def api_test():
    try:
        result = await version_config()
        if result:
            return JSONResponse({
                "ok": True,
                "message": "Server reachable",
                "version": result[0],
                "remote": result[1]
            })
        return JSONResponse({"ok": False, "message": "Cannot reach server"})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


# ============================================================
#  ULTIMATE DASHBOARD HTML
# ============================================================

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="theme-color" content="#0a0e1a">
<title>FF Level Up Bot — by NIROB</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}

:root{
  --bg-0:#070a13;
  --bg-1:#0d1220;
  --bg-2:#141a2e;
  --bg-3:#1c2440;
  --bg-4:#242e50;
  --border:#232c48;
  --border-2:#2f3a5c;
  --text-0:#eef2fb;
  --text-1:#9aa6c4;
  --text-2:#5e6a8a;
  --accent:#ff6b35;
  --accent-2:#ff8a5c;
  --accent-3:#ff3b5c;
  --accent-glow:rgba(255,107,53,.45);
  --success:#10d77e;
  --warning:#ffb800;
  --error:#ff3b5c;
  --info:#4d9dff;
  --purple:#a855f7;
  --gradient:linear-gradient(135deg,#ff6b35 0%,#ff3b5c 100%);
  --gradient-2:linear-gradient(135deg,#4d9dff 0%,#a855f7 100%);
  --gradient-success:linear-gradient(135deg,#10d77e 0%,#06b6d4 100%);
}

html,body{height:100%}

body{
  font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:var(--bg-0);
  color:var(--text-0);
  min-height:100vh;
  overflow-x:hidden;
  background-image:
    radial-gradient(ellipse 80% 50% at 50% -20%,rgba(255,107,53,.18),transparent),
    radial-gradient(ellipse 60% 50% at 80% 100%,rgba(77,157,255,.10),transparent),
    radial-gradient(ellipse 60% 50% at 0% 50%,rgba(168,85,247,.08),transparent);
  background-attachment:fixed;
  font-size:14px;
  line-height:1.5;
}

/* Animated background particles */
body::before{
  content:'';
  position:fixed;
  inset:0;
  background-image:
    radial-gradient(2px 2px at 20% 30%,rgba(255,107,53,.4),transparent),
    radial-gradient(2px 2px at 60% 70%,rgba(77,157,255,.3),transparent),
    radial-gradient(2px 2px at 80% 20%,rgba(168,85,247,.3),transparent);
  background-size:200px 200px,300px 300px,250px 250px;
  animation:float 20s linear infinite;
  pointer-events:none;
  opacity:.5;
  z-index:0;
}

@keyframes float{
  0%{transform:translateY(0)}
  100%{transform:translateY(-200px)}
}

/* ============ HEADER ============ */
.header{
  padding:16px 20px;
  border-bottom:1px solid var(--border);
  background:rgba(13,18,32,.85);
  backdrop-filter:blur(24px);
  -webkit-backdrop-filter:blur(24px);
  position:sticky;
  top:0;
  z-index:100;
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}

.brand{display:flex;align-items:center;gap:12px;min-width:0}

.logo{
  width:48px;height:48px;border-radius:14px;
  background:var(--gradient);
  display:flex;align-items:center;justify-content:center;
  font-size:24px;
  box-shadow:0 8px 28px var(--accent-glow),inset 0 2px 4px rgba(255,255,255,.2);
  position:relative;overflow:hidden;
  flex-shrink:0;
  animation:logoPulse 3s ease-in-out infinite;
}

@keyframes logoPulse{
  0%,100%{box-shadow:0 8px 28px var(--accent-glow),inset 0 2px 4px rgba(255,255,255,.2)}
  50%{box-shadow:0 12px 40px var(--accent-glow),inset 0 2px 4px rgba(255,255,255,.2)}
}

.logo::after{
  content:'';
  position:absolute;inset:0;
  background:linear-gradient(45deg,transparent 30%,rgba(255,255,255,.35) 50%,transparent 70%);
  animation:shine 3s infinite;
}

@keyframes shine{
  0%{transform:translateX(-100%)}
  100%{transform:translateX(100%)}
}

.brand-text{min-width:0}
.brand-text h1{
  font-size:17px;font-weight:800;letter-spacing:-.3px;
  background:linear-gradient(135deg,#fff,#b8c0d8);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.brand-text p{
  font-size:10px;color:var(--accent);font-weight:700;
  letter-spacing:1.5px;text-transform:uppercase;
  white-space:nowrap;
}

.header-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}

.status-pill{
  display:flex;align-items:center;gap:8px;
  padding:8px 14px;border-radius:100px;
  background:var(--bg-2);border:1px solid var(--border);
  font-size:12px;font-weight:700;
  transition:all .3s;
}

.status-pill.running{border-color:rgba(16,215,126,.4);background:rgba(16,215,126,.08)}
.status-pill.stopped{border-color:rgba(255,59,92,.4);background:rgba(255,59,92,.08)}

.status-dot{
  width:8px;height:8px;border-radius:50%;
  background:var(--text-2);position:relative;
}

.status-dot.running{
  background:var(--success);
  box-shadow:0 0 12px var(--success),0 0 24px var(--success);
  animation:pulse 1.5s infinite;
}

.status-dot.stopped{background:var(--error);box-shadow:0 0 12px var(--error)}
.status-dot.idle{background:var(--warning);box-shadow:0 0 12px var(--warning)}

@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* ============ CONTAINER ============ */
.container{max-width:1600px;margin:0 auto;padding:20px;position:relative;z-index:1}

/* ============ STATS ============ */
.stats-grid{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
  gap:12px;margin-bottom:20px;
}

.stat-card{
  background:linear-gradient(145deg,var(--bg-1),var(--bg-2));
  border:1px solid var(--border);
  border-radius:16px;
  padding:16px;
  position:relative;overflow:hidden;
  transition:transform .3s,border-color .3s,box-shadow .3s;
}

.stat-card:hover{
  transform:translateY(-3px);
  border-color:var(--accent);
  box-shadow:0 12px 32px rgba(255,107,53,.15);
}

.stat-card::before{
  content:'';position:absolute;
  top:0;left:0;right:0;height:3px;
  background:var(--gradient);opacity:.8;
}

.stat-card.blue::before{background:var(--gradient-2)}
.stat-card.green::before{background:var(--gradient-success)}

.stat-icon{
  font-size:20px;margin-bottom:8px;
  display:inline-block;
}

.stat-label{
  font-size:10px;text-transform:uppercase;letter-spacing:1.5px;
  color:var(--text-2);font-weight:800;margin-bottom:8px;
}

.stat-value{
  font-size:28px;font-weight:900;letter-spacing:-1px;
  background:linear-gradient(135deg,#fff,#9aa6c4);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
  font-variant-numeric:tabular-nums;
  line-height:1;
}

.stat-value.accent{
  background:var(--gradient);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
}

.stat-value.blue{
  background:var(--gradient-2);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
}

.stat-value.green{
  background:var(--gradient-success);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
}

.stat-sub{font-size:11px;color:var(--text-2);margin-top:6px;font-weight:500}

/* ============ MAIN GRID ============ */
.main-grid{
  display:grid;
  grid-template-columns:400px 1fr;
  gap:16px;
  align-items:start;
}

@media(max-width:1100px){.main-grid{grid-template-columns:1fr}}

/* ============ PANEL ============ */
.panel{
  background:linear-gradient(145deg,var(--bg-1),rgba(20,26,46,.6));
  border:1px solid var(--border);
  border-radius:18px;
  overflow:hidden;
  box-shadow:0 4px 24px rgba(0,0,0,.3);
}

.panel-header{
  padding:16px 20px;
  border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;
  background:linear-gradient(180deg,var(--bg-2),rgba(20,26,46,.3));
}

.panel-title{
  font-size:13px;font-weight:800;
  letter-spacing:.5px;
  display:flex;align-items:center;gap:8px;
  text-transform:uppercase;
}

.panel-title .icon{color:var(--accent);font-size:16px}

.panel-body{padding:20px}

/* ============ FORM ============ */
.form-group{margin-bottom:14px}

.form-label{
  display:block;font-size:10px;font-weight:800;
  text-transform:uppercase;letter-spacing:1.2px;
  color:var(--text-2);margin-bottom:6px;
}

.form-input{
  width:100%;padding:12px 14px;
  background:var(--bg-0);
  border:1px solid var(--border);
  border-radius:10px;
  color:var(--text-0);
  font-family:'JetBrains Mono',monospace;
  font-size:13px;outline:none;
  transition:border-color .2s,box-shadow .2s;
}

.form-input:focus{
  border-color:var(--accent);
  box-shadow:0 0 0 3px var(--accent-glow);
}

.form-input::placeholder{color:var(--text-2)}

/* ============ ACCOUNT LIST ============ */
.accounts-list{
  max-height:220px;overflow-y:auto;
  margin-bottom:12px;padding-right:4px;
}

.accounts-list::-webkit-scrollbar{width:5px}
.accounts-list::-webkit-scrollbar-track{background:var(--bg-0);border-radius:3px}
.accounts-list::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
.accounts-list::-webkit-scrollbar-thumb:hover{background:var(--accent)}

.account-item{
  display:flex;align-items:center;gap:10px;
  padding:10px 12px;
  background:var(--bg-2);
  border:1px solid var(--border);
  border-radius:10px;
  margin-bottom:6px;
  transition:all .2s;
  animation:slideIn .3s ease-out;
}

.account-item:hover{
  border-color:var(--accent);
  background:var(--bg-3);
  transform:translateX(2px);
}

@keyframes slideIn{from{opacity:0;transform:translateX(-8px)}to{opacity:1;transform:translateX(0)}}

.account-avatar{
  width:34px;height:34px;border-radius:10px;
  background:var(--gradient);
  display:flex;align-items:center;justify-content:center;
  font-size:14px;font-weight:800;color:#fff;
  flex-shrink:0;
}

.account-info{flex:1;min-width:0}
.account-uid{
  color:var(--text-0);font-weight:700;
  font-family:'JetBrains Mono',monospace;
  font-size:12px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.account-pass{
  color:var(--text-2);font-size:10px;
  font-family:'JetBrains Mono',monospace;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}

.btn-remove{
  background:transparent;border:1px solid var(--border);
  color:var(--error);
  width:28px;height:28px;border-radius:8px;
  cursor:pointer;font-size:14px;
  display:flex;align-items:center;justify-content:center;
  transition:all .2s;flex-shrink:0;
}

.btn-remove:hover{
  background:var(--error);color:#fff;
  border-color:var(--error);
  transform:scale(1.1);
}

/* ============ BUTTONS ============ */
.btn{
  width:100%;padding:13px 20px;border:none;
  border-radius:11px;
  font-family:'Inter',sans-serif;
  font-size:12px;font-weight:800;
  letter-spacing:1px;cursor:pointer;
  transition:all .2s;
  display:flex;align-items:center;justify-content:center;gap:8px;
  text-transform:uppercase;
  position:relative;overflow:hidden;
}

.btn::before{
  content:'';position:absolute;inset:0;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.15),transparent);
  transform:translateX(-100%);
  transition:transform .5s;
}

.btn:hover::before{transform:translateX(100%)}

.btn:disabled{opacity:.5;cursor:not-allowed}
.btn:disabled::before{display:none}

.btn-primary{
  background:var(--gradient);color:#fff;
  box-shadow:0 6px 20px var(--accent-glow);
}
.btn-primary:not(:disabled):hover{
  transform:translateY(-2px);
  box-shadow:0 12px 32px var(--accent-glow);
}

.btn-secondary{
  background:var(--bg-2);color:var(--text-0);
  border:1px solid var(--border);
}
.btn-secondary:not(:disabled):hover{
  background:var(--bg-3);border-color:var(--accent);
}

.btn-danger{
  background:linear-gradient(135deg,#ff3b5c,#c9184a);
  color:#fff;
  box-shadow:0 6px 20px rgba(255,59,92,.4);
}
.btn-danger:not(:disabled):hover{
  transform:translateY(-2px);
  box-shadow:0 12px 32px rgba(255,59,92,.5);
}

.btn-success{
  background:var(--gradient-success);color:#fff;
  box-shadow:0 6px 20px rgba(16,215,126,.4);
}

.btn-row{display:flex;gap:8px;margin-top:8px}

/* ============ LOGS ============ */
.tabs{
  display:flex;gap:4px;padding:4px;
  background:var(--bg-2);border-radius:12px;
  margin-bottom:14px;
}

.tab{
  flex:1;padding:10px 14px;
  border-radius:9px;
  font-size:11px;font-weight:800;
  letter-spacing:.5px;text-align:center;
  cursor:pointer;color:var(--text-1);
  transition:all .2s;text-transform:uppercase;
  position:relative;
}

.tab .badge{
  display:inline-block;
  background:var(--accent);
  color:#fff;
  font-size:9px;
  padding:1px 6px;
  border-radius:100px;
  margin-left:6px;
  font-weight:900;
}

.tab.active{
  background:var(--gradient);
  color:#fff;
  box-shadow:0 4px 12px var(--accent-glow);
}

.tab:not(.active):hover{color:var(--text-0);background:var(--bg-3)}

.tab-content{display:none}
.tab-content.active{display:block}

.logs-container{
  background:var(--bg-0);
  border-radius:12px;
  height:560px;overflow-y:auto;
  padding:14px;
  font-family:'JetBrains Mono',monospace;
  font-size:12px;line-height:1.7;
  border:1px solid var(--border);
}

.logs-container::-webkit-scrollbar{width:8px}
.logs-container::-webkit-scrollbar-track{background:var(--bg-0);border-radius:4px}
.logs-container::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
.logs-container::-webkit-scrollbar-thumb:hover{background:var(--accent)}

.log-entry{
  display:flex;gap:8px;
  padding:3px 0;
  border-bottom:1px solid rgba(35,44,72,.3);
  animation:slideIn .15s ease-out;
  word-break:break-word;
}

.log-time{color:var(--text-2);flex-shrink:0;font-size:11px}
.log-level{
  font-weight:800;flex-shrink:0;width:66px;
  text-transform:uppercase;font-size:10px;
  padding-top:1px;
}
.log-msg{color:var(--text-0);flex:1;min-width:0}

.log-entry.info .log-level{color:var(--info)}
.log-entry.success .log-level{color:var(--success)}
.log-entry.warning .log-level{color:var(--warning)}
.log-entry.error .log-level{color:var(--error)}

.log-entry.success .log-msg{color:#b7f5d8}
.log-entry.error .log-msg{color:#ffc4d0}
.log-entry.warning .log-msg{color:#ffe5a3}

/* ============ ACCOUNTS LIVE ============ */
.acc-card{
  background:linear-gradient(145deg,var(--bg-2),var(--bg-3));
  border:1px solid var(--border);
  border-radius:14px;
  padding:14px 16px;
  margin-bottom:10px;
  display:grid;
  grid-template-columns:1fr auto;
  gap:12px;align-items:center;
  transition:all .2s;
  animation:slideIn .3s ease-out;
}

.acc-card:hover{
  border-color:var(--accent);
  transform:translateX(3px);
  box-shadow:0 8px 24px rgba(255,107,53,.15);
}

.acc-name{
  font-weight:800;font-size:14px;
  margin-bottom:5px;
  display:flex;align-items:center;gap:6px;
}

.acc-meta{
  font-size:10.5px;color:var(--text-2);
  font-family:'JetBrains Mono',monospace;
  display:flex;gap:10px;flex-wrap:wrap;
}

.acc-meta span{display:flex;align-items:center;gap:3px}

.acc-badge{
  padding:6px 14px;border-radius:100px;
  font-size:10px;font-weight:800;
  letter-spacing:1px;text-transform:uppercase;
}

.acc-badge.running{
  background:rgba(16,215,126,.15);
  color:var(--success);
  border:1px solid rgba(16,215,126,.35);
  animation:pulse 2s infinite;
}

.acc-badge.connecting{
  background:rgba(255,184,0,.15);
  color:var(--warning);
  border:1px solid rgba(255,184,0,.35);
}

.acc-badge.idle{
  background:rgba(94,106,138,.15);
  color:var(--text-2);
  border:1px solid rgba(94,106,138,.35);
}

/* ============ EMPTY STATE ============ */
.empty-state{
  text-align:center;padding:40px 20px;
  color:var(--text-2);font-size:12px;
}
.empty-state .icon{
  font-size:42px;margin-bottom:12px;
  opacity:.4;display:block;
}
.empty-state .title{
  font-weight:700;color:var(--text-1);
  margin-bottom:4px;
}

/* ============ FOOTER ============ */
.footer{
  text-align:center;padding:32px 20px;
  color:var(--text-2);font-size:12px;
  border-top:1px solid var(--border);
  margin-top:32px;
  position:relative;z-index:1;
}

.footer .credit{
  color:var(--accent);font-weight:900;
  letter-spacing:1.5px;
  background:var(--gradient);
  -webkit-background-clip:text;
  -webkit-text-fill-color:transparent;
  background-clip:text;
}

/* ============ TOAST ============ */
.toast{
  position:fixed;bottom:24px;right:24px;
  padding:14px 20px;
  background:var(--bg-2);
  border:1px solid var(--border);
  border-radius:12px;
  font-size:13px;font-weight:700;
  box-shadow:0 12px 40px rgba(0,0,0,.5);
  transform:translateY(120px);
  opacity:0;
  transition:all .35s cubic-bezier(.34,1.56,.64,1);
  z-index:1000;max-width:360px;
  backdrop-filter:blur(20px);
}

.toast.show{transform:translateY(0);opacity:1}
.toast.success{border-color:var(--success);color:var(--success);background:rgba(16,215,126,.1)}
.toast.error{border-color:var(--error);color:var(--error);background:rgba(255,59,92,.1)}
.toast.info{border-color:var(--info);color:var(--info);background:rgba(77,157,255,.1)}
.toast.warning{border-color:var(--warning);color:var(--warning);background:rgba(255,184,0,.1)}

/* ============ MODAL ============ */
.modal-overlay{
  position:fixed;inset:0;
  background:rgba(7,10,19,.85);
  backdrop-filter:blur(8px);
  display:none;
  align-items:center;justify-content:center;
  z-index:200;
  padding:20px;
  animation:fadeIn .2s;
}

.modal-overlay.show{display:flex}

@keyframes fadeIn{from{opacity:0}to{opacity:1}}

.modal{
  background:var(--bg-1);
  border:1px solid var(--border);
  border-radius:18px;
  padding:28px;
  max-width:420px;
  width:100%;
  box-shadow:0 24px 64px rgba(0,0,0,.6);
  animation:scaleIn .3s cubic-bezier(.34,1.56,.64,1);
}

@keyframes scaleIn{from{transform:scale(.9);opacity:0}to{transform:scale(1);opacity:1}}

.modal h3{
  font-size:18px;font-weight:800;
  margin-bottom:8px;text-align:center;
}

.modal p{
  color:var(--text-1);font-size:13px;
  text-align:center;margin-bottom:22px;
  line-height:1.6;
}

.modal-icon{
  font-size:48px;text-align:center;
  display:block;margin-bottom:14px;
}

.modal-actions{
  display:flex;gap:10px;
}

/* ============ MOBILE ============ */
@media(max-width:640px){
  .header{padding:12px 14px}
  .container{padding:14px}
  .stat-value{font-size:22px}
  .brand-text h1{font-size:15px}
  .brand-text p{font-size:9px}
  .logo{width:42px;height:42px;font-size:20px}
  .btn{font-size:11px;padding:12px 14px}
  .panel-body{padding:16px}
  .logs-container{height:440px;font-size:11px}
  .stat-card{padding:14px}
  .toast{right:14px;left:14px;bottom:14px;max-width:none}
  .acc-meta{font-size:10px;gap:6px}
  .status-pill{padding:6px 12px;font-size:11px}
}

/* Scrollbar global */
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-track{background:var(--bg-0)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:var(--accent)}
</style>
</head>
<body>

<div class="header">
  <div class="brand">
    <div class="logo">🔥</div>
    <div class="brand-text">
      <h1>FF Level Up Bot</h1>
      <p>Ultimate Edition • by NIROB</p>
    </div>
  </div>
  <div class="header-actions">
    <div class="status-pill" id="statusPill">
      <span class="status-dot idle" id="statusDot"></span>
      <span id="statusText">Idle</span>
    </div>
  </div>
</div>

<div class="container">
  <div class="stats-grid">
    <div class="stat-card">
      <div class="stat-icon">⚡</div>
      <div class="stat-label">Bot Status</div>
      <div class="stat-value accent" id="statStatus">IDLE</div>
      <div class="stat-sub" id="statUptime">Not started</div>
    </div>
    <div class="stat-card blue">
      <div class="stat-icon">👥</div>
      <div class="stat-label">Accounts</div>
      <div class="stat-value blue" id="statAccounts">0</div>
      <div class="stat-sub">Connected workers</div>
    </div>
    <div class="stat-card green">
      <div class="stat-icon">🎯</div>
      <div class="stat-label">Total Matches</div>
      <div class="stat-value green" id="statMatches">0</div>
      <div class="stat-sub">Completed runs</div>
    </div>
    <div class="stat-card">
      <div class="stat-icon">🔥</div>
      <div class="stat-label">Active Matches</div>
      <div class="stat-value accent" id="statActive">0</div>
      <div class="stat-sub">In progress</div>
    </div>
  </div>

  <div class="main-grid">
    <!-- CONTROL PANEL -->
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title"><span class="icon">⚙️</span> Control Panel</div>
      </div>
      <div class="panel-body">
        <div class="form-group">
          <label class="form-label">UID</label>
          <input type="text" class="form-input" id="uidInput" placeholder="Enter Free Fire UID" autocomplete="off">
        </div>
        <div class="form-group">
          <label class="form-label">Password</label>
          <input type="text" class="form-input" id="passInput" placeholder="Enter password" autocomplete="off">
        </div>
        <button class="btn btn-secondary" id="addBtn">➕ Add Account</button>

        <div class="form-label" style="margin-top:16px;margin-bottom:8px;">
          Queue (<span id="queueCount">0</span>)
        </div>
        <div class="accounts-list" id="accountsList">
          <div class="empty-state">
            <span class="icon">📋</span>
            <div class="title">No accounts added</div>
            <div>Add UID and password above</div>
          </div>
        </div>

        <div class="btn-row" style="margin-top:14px">
          <button class="btn btn-primary" id="startBtn">🚀 Start Bot</button>
        </div>
        <div class="btn-row">
          <button class="btn btn-danger" id="stopBtn" disabled>🛑 Stop Bot</button>
        </div>
        <div class="btn-row">
          <button class="btn btn-secondary" id="testBtn">🔍 Test Connection</button>
        </div>
      </div>
    </div>

    <!-- LIVE MONITOR -->
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title"><span class="icon">📡</span> Live Monitor</div>
        <button class="btn btn-secondary" id="clearLogs" style="width:auto;padding:6px 12px;font-size:10px">🗑 Clear</button>
      </div>
      <div class="panel-body">
        <div class="tabs">
          <div class="tab active" data-tab="logs">Logs <span class="badge" id="logBadge">0</span></div>
          <div class="tab" data-tab="accounts">Accounts <span class="badge" id="accBadge">0</span></div>
        </div>

        <div class="tab-content active" id="tab-logs">
          <div class="logs-container" id="logsContainer">
            <div class="empty-state">
              <span class="icon">📜</span>
              <div class="title">Waiting for logs</div>
              <div>Start the bot to see activity</div>
            </div>
          </div>
        </div>

        <div class="tab-content" id="tab-accounts">
          <div id="accountsLive">
            <div class="empty-state">
              <span class="icon">👤</span>
              <div class="title">No active accounts</div>
              <div>Login in progress...</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">
    Crafted with <span style="color:var(--error)">♥</span> by <span class="credit">NIROB</span> • © 2024 All Rights Reserved
  </div>
</div>

<!-- Confirm Modal -->
<div class="modal-overlay" id="confirmModal">
  <div class="modal">
    <span class="modal-icon" id="modalIcon">⚠️</span>
    <h3 id="modalTitle">Confirm Action</h3>
    <p id="modalText">Are you sure?</p>
    <div class="modal-actions">
      <button class="btn btn-secondary" id="modalCancel">Cancel</button>
      <button class="btn btn-danger" id="modalConfirm">Confirm</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
(function(){
  'use strict';

  const state = {
    accounts: [],
    logs: [],
    stats: {bot_status:'idle',active_matches:0,total_matches:0,uptime_start:0,accounts:{}},
    ws: null,
    wsTimer: null,
    logCount: 0
  };

  const $ = id => document.getElementById(id);

  // ============ TOAST ============
  let toastTimer = null;
  function toast(msg, type='info'){
    const el = $('toast');
    el.textContent = msg;
    el.className = `toast show ${type}`;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(()=>{ el.className = 'toast'; }, 3400);
  }

  // ============ CONFIRM MODAL ============
  function confirmAction(icon, title, text, onConfirm){
    $('modalIcon').textContent = icon;
    $('modalTitle').textContent = title;
    $('modalText').textContent = text;
    $('confirmModal').classList.add('show');
    const cleanup = () => {
      $('confirmModal').classList.remove('show');
      $('modalConfirm').onclick = null;
      $('modalCancel').onclick = null;
    };
    $('modalCancel').onclick = cleanup;
    $('modalConfirm').onclick = () => { cleanup(); onConfirm(); };
  }

  // ============ WEBSOCKET ============
  function connectWS(){
    if(state.ws && state.ws.readyState === WebSocket.OPEN) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws`;
    try { state.ws = new WebSocket(url); }
    catch(e){ scheduleReconnect(); return; }

    state.ws.onopen = () => {
      console.log('[WS] connected');
      clearTimeout(state.wsTimer);
    };

    state.ws.onmessage = ev => {
      try { handleMessage(JSON.parse(ev.data)); } catch(e){}
    };

    state.ws.onclose = () => {
      console.log('[WS] closed, reconnecting...');
      scheduleReconnect();
    };

    state.ws.onerror = () => {
      try { state.ws.close(); } catch(e){}
    };
  }

  function scheduleReconnect(){
    clearTimeout(state.wsTimer);
    state.wsTimer = setTimeout(connectWS, 2000);
  }

  function handleMessage(msg){
    if(msg.type === 'log'){
      addLog(msg.data);
    } else if(msg.type === 'history'){
      (msg.data || []).forEach(addLog);
      if(msg.stats){ state.stats = msg.stats; renderStats(); }
    } else if(msg.type === 'stats'){
      state.stats = msg.data;
      renderStats();
    }
  }

  // ============ LOGS ============
  function addLog(entry){
    state.logCount++;
    state.logs.push(entry);
    if(state.logs.length > 800) state.logs.shift();
    $('logBadge').textContent = state.logCount;

    const cont = $('logsContainer');
    const emptyState = cont.querySelector('.empty-state');
    if(emptyState) cont.innerHTML = '';

    const div = document.createElement('div');
    div.className = `log-entry ${entry.level || 'info'}`;
    div.innerHTML = `
      <span class="log-time">${entry.time || ''}</span>
      <span class="log-level">${(entry.level || 'info').toUpperCase()}</span>
      <span class="log-msg">${escapeHtml(entry.message || '')}</span>
    `;
    cont.appendChild(div);

    // Auto-scroll if near bottom
    if(cont.scrollHeight - cont.scrollTop - cont.clientHeight < 150){
      cont.scrollTop = cont.scrollHeight;
    }

    while(cont.children.length > 800) cont.removeChild(cont.firstChild);
  }

  function escapeHtml(s){
    return String(s)
      .replace(/&/g,'&amp;')
      .replace(/</g,'&lt;')
      .replace(/>/g,'&gt;')
      .replace(/"/g,'&quot;')
      .replace(/'/g,'&#39;');
  }

  // ============ RENDER STATS ============
  function renderStats(){
    const s = state.stats;
    const botStatus = (s.bot_status || 'idle').toLowerCase();

    $('statStatus').textContent = botStatus.toUpperCase();
    $('statusText').textContent = botStatus.charAt(0).toUpperCase() + botStatus.slice(1);

    const dot = $('statusDot');
    const pill = $('statusPill');
    dot.className = 'status-dot ' + (botStatus === 'running' ? 'running' : (botStatus === 'stopped' ? 'stopped' : 'idle'));
    pill.className = 'status-pill ' + (botStatus === 'running' ? 'running' : (botStatus === 'stopped' ? 'stopped' : ''));

    const accCount = Object.keys(s.accounts || {}).length;
    $('statAccounts').textContent = accCount;
    $('statMatches').textContent = s.total_matches || 0;
    $('statActive').textContent = s.active_matches || 0;
    $('accBadge').textContent = accCount;

    if(s.uptime_start && botStatus === 'running'){
      updateUptime();
    } else {
      $('statUptime').textContent = 'Not started';
    }

    const isRunning = botStatus === 'running';
    $('startBtn').disabled = isRunning;
    $('stopBtn').disabled = !isRunning;

    renderLiveAccounts(s.accounts || {});
  }

  function updateUptime(){
    if(state.stats.bot_status !== 'running' || !state.stats.uptime_start) return;
    const up = Math.floor((Date.now()/1000) - state.stats.uptime_start);
    const h = Math.floor(up/3600);
    const m = Math.floor((up%3600)/60);
    const sec = up%60;
    $('statUptime').textContent = `⏱ ${h}h ${m}m ${sec}s`;
  }

  setInterval(updateUptime, 1000);

  function renderLiveAccounts(accounts){
    const cont = $('accountsLive');
    const keys = Object.keys(accounts);
    if(keys.length === 0){
      cont.innerHTML = `<div class="empty-state">
        <span class="icon">👤</span>
        <div class="title">No active accounts</div>
        <div>Login in progress...</div>
      </div>`;
      return;
    }
    cont.innerHTML = keys.map(uid => {
      const a = accounts[uid];
      const status = (a.status || 'connecting').toLowerCase();
      const initial = (a.nickname || uid).charAt(0).toUpperCase();
      return `
        <div class="acc-card">
          <div style="display:flex;align-items:center;gap:12px;min-width:0">
            <div class="account-avatar">${escapeHtml(initial)}</div>
            <div style="min-width:0">
              <div class="acc-name">${escapeHtml(a.nickname || uid)}</div>
              <div class="acc-meta">
                <span>🆔 ${escapeHtml(uid)}</span>
                <span>⭐ Lvl ${a.level || 1}</span>
                <span>💎 ${a.exp || 0}</span>
                <span>🌍 ${a.region || 'ME'}</span>
                ${a.matches ? `<span>🎯 ${a.matches}</span>` : ''}
              </div>
            </div>
          </div>
          <div class="acc-badge ${status}">${status}</div>
        </div>
      `;
    }).join('');
  }

  // ============ QUEUE ============
  function renderQueue(){
    const cont = $('accountsList');
    $('queueCount').textContent = state.accounts.length;
    if(state.accounts.length === 0){
      cont.innerHTML = `<div class="empty-state">
        <span class="icon">📋</span>
        <div class="title">No accounts added</div>
        <div>Add UID and password above</div>
      </div>`;
      return;
    }
    cont.innerHTML = state.accounts.map((a, i) => {
      const initial = (a.uid || '?').charAt(0).toUpperCase();
      return `
        <div class="account-item">
          <div class="account-avatar">${escapeHtml(initial)}</div>
          <div class="account-info">
            <div class="account-uid">${escapeHtml(a.uid)}</div>
            <div class="account-pass">${escapeHtml('•'.repeat(Math.min(a.password.length, 20)))}</div>
          </div>
          <button class="btn-remove" data-idx="${i}" title="Remove">✕</button>
        </div>
      `;
    }).join('');
    cont.querySelectorAll('.btn-remove').forEach(btn => {
      btn.addEventListener('click', () => {
        const idx = parseInt(btn.dataset.idx);
        const removed = state.accounts.splice(idx, 1)[0];
        renderQueue();
        toast(`Removed ${removed.uid}`, 'info');
      });
    });
  }

  // ============ EVENT: ADD ============
  $('addBtn').addEventListener('click', () => {
    const uid = $('uidInput').value.trim();
    const password = $('passInput').value.trim();
    if(!uid || !password){
      toast('UID and Password required', 'error');
      return;
    }
    if(!/^\d{6,12}$/.test(uid)){
      toast('UID must be 6-12 digits', 'warning');
      return;
    }
    if(state.accounts.some(a => a.uid === uid)){
      toast('UID already in queue', 'warning');
      return;
    }
    state.accounts.push({uid, password});
    $('uidInput').value = '';
    $('passInput').value = '';
    $('uidInput').focus();
    renderQueue();
    toast(`✓ Added ${uid}`, 'success');
  });

  $('passInput').addEventListener('keydown', e => {
    if(e.key === 'Enter') $('addBtn').click();
  });
  $('uidInput').addEventListener('keydown', e => {
    if(e.key === 'Enter') $('passInput').focus();
  });

  // ============ EVENT: START ============
  $('startBtn').addEventListener('click', () => {
    if(state.accounts.length === 0){
      toast('Add at least one account', 'error');
      return;
    }
    confirmAction('🚀', 'Start Bot?',
      `Launch bot with ${state.accounts.length} account(s)?`,
      async () => {
        $('startBtn').disabled = true;
        try {
          const res = await fetch('/api/start', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({accounts: state.accounts})
          });
          const data = await res.json();
          toast(data.message || (data.ok ? 'Bot started' : 'Failed'), data.ok ? 'success' : 'error');
          if(!data.ok) $('startBtn').disabled = false;
        } catch(e){
          toast('Network error: ' + e.message, 'error');
          $('startBtn').disabled = false;
        }
      });
  });

  // ============ EVENT: STOP ============
  $('stopBtn').addEventListener('click', () => {
    confirmAction('🛑', 'Stop Bot?',
      'This will stop all running workers.',
      async () => {
        $('stopBtn').disabled = true;
        try {
          const res = await fetch('/api/stop', {method:'POST'});
          const data = await res.json();
          toast(data.message || (data.ok ? 'Bot stopped' : 'Failed'), data.ok ? 'info' : 'error');
        } catch(e){
          toast('Network error: ' + e.message, 'error');
        }
      });
  });

  // ============ EVENT: TEST ============
  $('testBtn').addEventListener('click', async () => {
    $('testBtn').disabled = true;
    toast('Testing server...', 'info');
    try {
      const res = await fetch('/api/test-connection', {method:'POST'});
      const data = await res.json();
      if(data.ok){
        toast(`✓ Server OK • v${data.version}`, 'success');
      } else {
        toast('✗ ' + (data.message || 'Failed'), 'error');
      }
    } catch(e){
      toast('Network error', 'error');
    } finally {
      $('testBtn').disabled = false;
    }
  });

  // ============ EVENT: CLEAR LOGS ============
  $('clearLogs').addEventListener('click', () => {
    state.logs = [];
    state.logCount = 0;
    $('logBadge').textContent = '0';
    $('logsContainer').innerHTML = `<div class="empty-state">
      <span class="icon">📜</span>
      <div class="title">Logs cleared</div>
      <div>New logs will appear here</div>
    </div>`;
    toast('Logs cleared', 'info');
  });

  // ============ TABS ============
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      const target = $('tab-' + tab.dataset.tab);
      if(target) target.classList.add('active');
    });
  });

  // ============ INIT ============
  fetch('/api/status').then(r => r.json()).then(data => {
    if(data.stats){ state.stats = data.stats; renderStats(); }
    if(data.bot && data.bot.accounts && data.bot.accounts.length){
      state.accounts = data.bot.accounts;
      renderQueue();
    }
  }).catch(() => {});

  connectWS();
  renderQueue();
})();
</script>
</body>
</html>
"""


# ============================================================
#  ENTRYPOINT
# ============================================================

def run_server():
    print("\n" + "═" * 64)
    print("   🔥 FF LEVEL UP BOT — ULTIMATE EDITION")
    print("   👑 Created by: NIROB")
    print("═" * 64)
    print(f"   🌐 Dashboard:  http://127.0.0.1:8080")
    print(f"   📡 WebSocket:  ws://127.0.0.1:8080/ws")
    print(f"   📚 API Docs:   http://127.0.0.1:8080/docs")
    print("═" * 64)
    print("   Press CTRL+C to stop the server")
    print("═" * 64 + "\n")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="warning",
        access_log=False
    )


if __name__ == "__main__":
    try:
        run_server()
    except KeyboardInterrupt:
        print("\n👋 Server stopped. Goodbye!")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()
