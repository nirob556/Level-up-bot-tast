#!/usr/bin/env python3
import os
import sys
import json
import time
import random
import hashlib
import hmac
import threading
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# GLOBAL RATE LIMITER  (protects against HTTP 429)
# ============================================================
class RateLimiter:
    """Ensures a minimum gap between ALL outbound HTTP requests."""
    def __init__(self, min_gap_sec=0.6):
        self.min_gap = min_gap_sec
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_gap:
                time.sleep(self.min_gap - delta)
            self._last = time.time()

    def set_gap(self, gap):
        with self._lock:
            self.min_gap = gap


RATE = RateLimiter(min_gap_sec=0.2)

# ============================================================
# PROXY POOL
# ============================================================
PROXY_FILE = "proxies.txt"
_proxy_list = []
_proxy_lock = threading.Lock()
_proxy_idx = 0

def load_proxies():
    global _proxy_list
    try:
        with open(PROXY_FILE) as f:
            _proxy_list = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    except FileNotFoundError:
        _proxy_list = []
    if _proxy_list:
        log_ok(f"Loaded {len(_proxy_list)} proxies from {PROXY_FILE}")
    else:
        log_warn(f"No proxies found ({PROXY_FILE}). Running direct — will hit 429 fast.")

def next_proxy():
    global _proxy_idx
    if not _proxy_list:
        return None
    with _proxy_lock:
        p = _proxy_list[_proxy_idx % len(_proxy_list)]
        _proxy_idx += 1
        return p

def proxy_dict(p):
    if not p:
        return None
    return {"http": p, "https": p}

def attach_proxy(session):
    p = next_proxy()
    if p:
        session.proxies.update(proxy_dict(p))
        return p
    return None




# ============================================================
# RJB COLOR ENGINE  (Rainbow Just Brilliant)
# ============================================================
class RJB:
    PALETTE = [
        "\033[91m",  # bright red
        "\033[93m",  # bright yellow
        "\033[92m",  # bright green
        "\033[96m",  # bright cyan
        "\033[94m",  # bright blue
        "\033[95m",  # bright magenta
        "\033[97m",  # bright white
    ]
    RESET = "\033[0m"
    BOLD  = "\033[1m"
    DIM   = "\033[2m"

    _cycle = itertools.cycle(PALETTE)

    @classmethod
    def next_color(cls):
        return next(cls._cycle)

    @classmethod
    def paint(cls, text):
        return cls.next_color() + text + cls.RESET

    @classmethod
    def paint_static(cls, text, idx):
        return cls.PALETTE[idx % len(cls.PALETTE)] + text + cls.RESET


# ============================================================
# LOGGING
# ============================================================
def _stamp():
    return RJB.paint_static(time.strftime("%H:%M:%S"), 5)

def log_info(m):  print(f"{_stamp()} {RJB.paint('[i]')} {RJB.paint(m)}", flush=True)
def log_ok(m):    print(f"{_stamp()} {RJB.paint('[+]')} {RJB.BOLD}{RJB.paint(m)}", flush=True)
def log_warn(m):  print(f"{_stamp()} {RJB.paint('[!]')} {RJB.paint(m)}", flush=True)
def log_err(m):   print(f"{_stamp()} {RJB.paint('[-]')} {RJB.paint(m)}", flush=True)
def log_debug(m): print(f"{_stamp()} {RJB.DIM}    {m}{RJB.RESET}", flush=True)


def rainbow_line(text, width=60):
    out = ""
    for i, ch in enumerate(text.ljust(width)):
        out += RJB.paint_static(ch, i)
    print(out)


import os

def banner():
    os.system("cls" if os.name == "nt" else "clear")
    print()

    cyan_color = "\033[1;96m"
    yellow_color = "\033[1;93m"
    s_ = "\033[0m"

    ascii_art = f"""{cyan_color}        ██╗  ██╗███████╗██████╗  ██████╗ ██╗  ██╗
        ╚██╗██╔╝██╔════╝██╔══██╗██╔═══██╗╚██╗██╔╝
         ╚███╔╝ █████╗  ██████╔╝██║   ██║ ╚███╔╝ 
         ██╔██╗ ██╔══╝  ██╔══██╗██║   ██║ ██╔██╗ 
        ██╔╝ ██╗███████╗██║  ██║╚██████╔╝██╔╝ ██╗
        ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝
{yellow_color}        ███╗   ███╗ ██████╗ ██████╗ ███████╗
        ████╗ ████║██╔═══██╗██╔══██╗██╔════╝
        ██╔████╔██║██║   ██║██║  ██║███████╗
        ██║╚██╔╝██║██║   ██║██║  ██║╚════██║
        ██║ ╚═╝ ██║╚██████╔╝██████╔╝███████║
        ╚═╝     ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝{s_}"""

    print(ascii_art)
    print(f" \033[1;92m👑\033[0m \033[1;93mDEVELOPER :\033[0m \033[1;96m@XEROX_MODS\033[0m")
    print(f" \033[1;93m⚡\033[0m \033[1;93mTOOL      :\033[0m \033[1;96mFF Guest Generator & Detector (OB55 Multi-Threaded)\033[0m")
    print(f" \033[1;96m🎮\033[0m \033[1;93mGAME      :\033[0m \033[1;96mFree Fire\033[0m")
    print(f" \033[1;92m🔵\033[0m \033[1;93mVERSION   :\033[0m \033[1;96mOB55 Pro Plus\033[0m")

    print()

# ============================================================
# SPINNER
# ============================================================
SPINNER_FRAMES = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]


class Spinner:
    def __init__(self, message="Working"):
        self.message = message
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()

    def _spin(self):
        i = 0
        while not self._stop.is_set():
            frame = SPINNER_FRAMES[i % len(SPINNER_FRAMES)]
            color = RJB.PALETTE[i % len(RJB.PALETTE)]
            with self._lock:
                sys.stdout.write(f"\r{color}{frame}{RJB.RESET} {RJB.paint(self.message)}")
                sys.stdout.flush()
            i += 1
            time.sleep(0.08)

    def start(self):
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def stop(self, final_msg=None):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)
        with self._lock:
            sys.stdout.write("\r" + " " * 90 + "\r")
            sys.stdout.flush()
        if final_msg:
            print(final_msg)


class ProgressBar:
    def __init__(self, total, prefix="Progress", width=40):
        self.total = total
        self.done = 0
        self.prefix = prefix
        self.width = width
        self._lock = threading.Lock()

    def update(self, n=1):
        with self._lock:
            self.done += n
            self._render()

    def _render(self):
        pct = self.done / max(1, self.total)
        filled = int(self.width * pct)
        bar = ""
        for i in range(self.width):
            ch = "█" if i < filled else "░"
            bar += RJB.paint_static(ch, i)
        line = f"{self.prefix} |{bar}| {self.done}/{self.total}"
        sys.stdout.write("\r" + line + " " * 5)
        sys.stdout.flush()

    def finish(self):
        self._render()
        sys.stdout.write("\n")
        sys.stdout.flush()


# ============================================================
# BOX UI
# ============================================================
def print_box(title, rows, width=62):
    print()
    top = "┌─ " + title + " " + "─" * (width - len(title) - 4) + "┐"
    print(RJB.paint(top))
    for k, v in rows:
        line = f"│ {k:<14}: {v}"
        pad = width - len(line) + 1
        line = line + " " * max(0, pad) + "│"
        print(RJB.paint(line))
    bottom = "└" + "─" * (width - 2) + "┘"
    print(RJB.paint(bottom))
    print()


def print_server_box(servers, selected_idx=0):
    print()

    # Smooth colors
    cyan = "\033[1;96m"
    blue = "\033[1;94m"
    purple = "\033[1;95m"
    yellow = "\033[1;93m"
    green = "\033[1;92m"
    white = "\033[1;97m"
    reset = "\033[0m"

    print(f"{cyan}╔{'═' * 58}╗{reset}")
    print(f"{blue}║{purple}   SELECT SERVER / REGION{blue}{' ' * 33}║{reset}")
    print(f"{cyan}╠{'═' * 58}╣{reset}")

    for i, s in enumerate(servers):
        code, name, host = s

        if i == selected_idx:
            # Selected server
            marker = f"{yellow}➤{reset}"
            num = f"{yellow}[{i+1:>2}]{reset}"
            code_color = green
            name_color = white
            host_color = cyan
        else:
            # Normal server
            marker = " "
            num = f"{blue}[{i+1:>2}]{reset}"
            code_color = cyan
            name_color = white
            host_color = purple

        print(
            f"║ {marker} "
            f"{num} "
            f"{code_color}{code:<4}{reset}  "
            f"{name_color}{name:<18}{reset} "
            f"{host_color}{host:<23}{reset}║"
        )

    print(f"{cyan}╚{'═' * 58}╝{reset}")
    print()

# ============================================================
# CONSTANTS
# ============================================================
AES_KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
AES_IV  = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

CLIENT_SECRET = "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3"
APP_ID        = 100067

OAUTH_REGISTER_URL    = "https://100067.connect.garena.com/api/v2/oauth/guest:register"
OAUTH_TOKEN_URL       = "https://100067.connect.garena.com/oauth/guest/token/grant"
MAJOR_REGISTER_URL    = "https://loginbp.ppmainecoonghj.com/MajorRegister"
NEWBIE_URL            = "https://loginbp.ppmainecoonghj.com/ChooseNewbieChoice"
MAJOR_LOGIN_URL       = "https://loginbp.ppmainecoonghj.com/MajorLogin"
GENERATE_NICKNAME_URL = "https://loginbp.ppmainecoonghj.com/GenerateNickname"

SERVERS = [
    ("IND", "India",         "clientbp.ind.freefiremobile.com"),
    ("BD",  "Bangladesh",    "clientbp.bd.freefiremobile.com"),
    ("SG",  "Singapore",     "clientbp.sg.freefiremobile.com"),
    ("ID",  "Indonesia",     "clientbp.id.freefiremobile.com"),
    ("TH",  "Thailand",      "clientbp.th.freefiremobile.com"),
    ("PK",  "Pakistan",      "clientbp.pk.freefiremobile.com"),
    ("VN",  "Vietnam",       "clientbp.vn.freefiremobile.com"),
    ("MY",  "Malaysia",      "clientbp.my.freefiremobile.com"),
    ("BR",  "Brazil",        "clientbp.br.freefiremobile.com"),
    ("US",  "United States", "clientbp.us.freefiremobile.com"),
]

OUTPUT_FILE = "accounts.json"

XOR_KEY = [0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,
           0x30,0x30,0x30,0x30,0x30,0x32,0x30,0x31,0x37,0x30,0x30,0x30,0x30,0x30,0x32,0x30]

FIELD_22_HEX = (
    "474752450101010062020000a78910bd098e3ff2e4345d59a31db114ea088f37e32e65"
    "212ff96621793d9eb78720d0bf2ac95176569765247ada5eb01376b3b9931794a4946"
    "d76ef8890779f3f2129317e6e1cb2fdcf7b06247cea343b8d4f167eff85a2e1dfe99"
    "b4583a7e1a155dbe7f7f85cff3b223eb77222ec1228f3ee1ef6ce7f8ca24b00a554"
    "e497328812f8df74c82d519ae3e3ceab436eb145e8a517089a7cef6a4efb22214b2"
    "a4b19989b74807584afe5e52825e7ac60e19a596a9bf02d961de6a0ed2515ec6023"
    "fdb7684d9464b97b21c527ce61b6bee4ef30d20a1fa33a996952d44d44e44f2f86c"
    "768aaf2a7808ad60f91048dca0207961ba7c2555c48341b30190debc775edb2cee24"
    "4cf51fca3760ce4388be2db45e80b813b5beb9c784007b7762d7a1e428affc8a3a4"
    "cd76cbaef648a274297fde33233dccd3f272cb77f39a1affe0365a24954111f768f72"
    "0e77535af024bea2b2726c3bac992374755c3deacf09235e6865d456e651680d115a"
    "751a797225eaacdf9a513cf104526e1a32e5296e111a33ae581a63850837921df848"
    "9adfd41ea895b7cf3f5b2e45d538a6e4f032f590ccbb7daf5fa9c50adadee0799661"
    "4c3f957bda349e6c484fdf55970d1943ad7955a76671298b6d98b636b69ebde6bc94"
    "dbd93ac3393a6ae230130445b2d744189167854a5617be2393e7d8fbb5719a1b4754"
    "1ba466167e3e05a6c244f1301ee2035acf94dffc8adbde747d5cd85e35ead3acc372"
    "a59c4220e54bf63f9d80485f3de2518495c1d0f78c911d2da595911fd2a1989cf17"
    "cf3ded5f6c92dea64d675c555c11df92d6c517ca5d0d61a8962f43f76ec7e87596c"
    "8325ebf9ab0f8e6d2eca33c511ceac6980906ebd68c478665591dcecf788ec6ef34c"
    "fbe3fcc279c6147c5bf91cd43cdc9704236"
)


CURRENT_REGION = "IND"
CURRENT_HOST   = "clientbp.ind.freefiremobile.com"


# ============================================================
# FILE WRITE LOCK
# ============================================================
_file_lock = threading.Lock()


def save_account(account):
    with _file_lock:
        try:
            with open(OUTPUT_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            data = []
        data.append(account)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# ============================================================
# PROTOBUF WRITER / READER
# ============================================================
def write_varint(value: int) -> bytes:
    out = []
    while value > 127:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def write_varint_field(f, v):
    return write_varint((f << 3) | 0) + write_varint(v)


def write_string_field(f, v):
    b = v.encode("utf-8") if isinstance(v, str) else v
    return write_varint((f << 3) | 2) + write_varint(len(b)) + b


def write_bytes_field(f, v):
    return write_varint((f << 3) | 2) + write_varint(len(v)) + v


def assemble_proto(fields):
    data = bytearray()
    for k, v in fields.items():
        if isinstance(v, int):
            data.extend(write_varint_field(k, v))
        elif isinstance(v, str):
            data.extend(write_string_field(k, v))
        elif isinstance(v, bytes):
            data.extend(write_bytes_field(k, v))
    return bytes(data)


def read_varint(data, offset):
    r, s = 0, 0
    while True:
        b = data[offset]; offset += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            break
        s += 7
    return r, offset


def parse_proto(data):
    out = {}
    off = 0
    while off < len(data):
        try:
            tag, off = read_varint(data, off)
        except Exception:
            break
        f = tag >> 3
        w = tag & 0x7
        if f == 0:
            break
        try:
            if w == 0:
                v, off = read_varint(data, off)
                out[f] = v
            elif w == 2:
                ln, off = read_varint(data, off)
                val = data[off:off + ln]
                off += ln
                try:
                    out[f] = val.decode("utf-8")
                except Exception:
                    out[f] = val.hex()
            else:
                break
        except Exception:
            break
    return out


# ============================================================
# AES
# ============================================================
def aes_encrypt(plain):
    return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).encrypt(pad(plain, AES.block_size))


def aes_decrypt(ciphertext):
    if not ciphertext or len(ciphertext) % 16 != 0:
        return ciphertext
    try:
        try:
            return unpad(AES.new(AES_KEY, AES.MODE_CBC, AES_IV).decrypt(ciphertext),
                         AES.block_size)
        except ValueError:
            return AES.new(AES_KEY, AES.MODE_CBC, AES_IV).decrypt(ciphertext)
    except Exception:
        return ciphertext


def xor_open_id(open_id: str) -> bytes:
    b = open_id.encode("utf-8")
    return bytes(x ^ XOR_KEY[i % len(XOR_KEY)] for i, x in enumerate(b))


def retry(func, retries=3, delay=1.0, backoff=2.0):
    last = None
    for attempt in range(1, retries + 1):
        try:
            RATE.wait()   # ← rate limit EVERY attempt
            return func()
        except Exception as e:
            last = e
            msg = str(e)
            # HTTP 429 -> long backoff
            if "429" in msg:
                wait = random.uniform(1.5, 3.0) if _proxy_list else random.uniform(8.0, 16.0)
                log_warn(f"rate-limited (429), waiting {wait:.1f}s ...")
                time.sleep(wait)
            elif attempt < retries:
                time.sleep(delay * (backoff ** (attempt - 1)))
    log_debug(f"retry exhausted: {last}")
    return None


# ============================================================
# STEP 1 — Register guest
# ============================================================
def register_guest(session, password):
    attach_proxy(session)
    RATE.wait()
    payload = {"app_id": APP_ID, "client_type": 2, "password": password, "source": 2}
    body = json.dumps(payload, separators=(",", ":"))
    sig = hmac.new(CLIENT_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    headers = {
        "User-Agent": "GarenaMSDK/4.0.42(KB2003 ;Android 13;en;HK;app 2.130.1 2019118332;)",
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "Authorization": f"Signature {sig}",
    }
    r = session.post(OAUTH_REGISTER_URL, data=body, headers=headers, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    d = r.json()
    if d.get("code") != 0:
        raise RuntimeError(f"register failed: {d.get('message')}")
    return str(d["data"]["uid"])


# ============================================================
# STEP 2 — Token grant
# ============================================================
def get_token(session, uid, password):
    attach_proxy(session)
    RATE.wait()
    payload = {
        "uid": uid, "password": password, "response_type": "token",
        "client_type": "2", "client_secret": CLIENT_SECRET, "client_id": "100067",
    }
    headers = {
        "User-Agent": "GarenaMSDK/4.0.30",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive",
    }
    r = session.post(OAUTH_TOKEN_URL, data=payload, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    d = r.json()
    at = d.get("access_token")
    oi = d.get("open_id")
    if not at or not oi:
        raise RuntimeError("token missing fields")
    return at, oi


# ============================================================
# STEP 3 — GenerateNickname
# ============================================================
def generate_nickname(session, open_id):
    attach_proxy(session)
    RATE.wait()
    plain = assemble_proto({1: "en", 2: open_id})
    encrypted = aes_encrypt(plain)
    headers = {
        "Accept-Encoding": "gzip",
        "Authorization": "Bearer",
        "Connection": "Keep-Alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Expect": "100-continue",
        "Host": GENERATE_NICKNAME_URL.split("/")[2],
        "ReleaseVersion": "OB55",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "X-GA": "v1 1",
        "X-Unity-Version": "2018.4.12f1",
    }
    r = session.post(GENERATE_NICKNAME_URL, data=encrypted, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    name = r.text.strip()
    if not name:
        raise RuntimeError("empty nickname")
    return name


# ============================================================
# STEP 4 — MajorRegister
# ============================================================
def send_major_register(session, nickname, access_token, open_id):
    attach_proxy(session)
    RATE.wait()
    fields = {
        1:  nickname,
        2:  access_token,
        3:  open_id,
        5:  102000007,
        6:  4,
        7:  1,
        13: 1,
        14: xor_open_id(open_id),
        15: CURRENT_REGION,
        16: 2,
        20: "2.133.8",
        21: 1,
        22: bytes.fromhex(FIELD_22_HEX),
    }
    encrypted = aes_encrypt(assemble_proto(fields))
    headers = {
        "Host":             MAJOR_REGISTER_URL.split("/")[2],
        "User-Agent":       "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept":           "*/*",
        "Accept-Encoding":  "deflate, gzip",
        "X-GA-SV":          str(int(time.time())),
        "Authorization":    "Bearer",
        "X-GA":             "v1 1",
        "ReleaseVersion":   "OB55",
        "Content-Type":     "application/x-www-form-urlencoded",
        "X-Unity-Version":  "2018.4.12f1",
        "Content-Length":   str(len(encrypted)),
    }
    r = session.post(MAJOR_REGISTER_URL, data=encrypted, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    parsed = parse_proto(r.content)
    aid = parsed.get(3)
    if not aid:
        raise RuntimeError("no account_id")
    return int(aid)


# ============================================================
# STEP 5 — ChooseNewbieChoice
# ============================================================
def send_newbie(session, account_id, choice=3):
    attach_proxy(session)
    RATE.wait()
    plain = assemble_proto({1: int(account_id), 2: 1, 3: int(choice)})
    encrypted = aes_encrypt(plain)
    headers = {
        "Host": "loginbp.ppmainecoonghj.com",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*",
        "Accept-Encoding": "deflate, gzip",
        "X-GA-SV": str(int(time.time())),
        "Authorization": "Bearer",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(encrypted)),
    }
    r = session.post(NEWBIE_URL, headers=headers, data=encrypted,
                     timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return True


# ============================================================
# STEP 6 — MajorLogin (extract JWT)
# ============================================================
def send_major_login(session, access_token, open_id, platform="4"):
    attach_proxy(session)
    RATE.wait()
    fields = {
        3: time.strftime("%Y-%m-%d %H:%M:%S"),
        4: "free fire", 5: 1, 7: "2.133.9",
        8: "Android OS 10 / API-29 (QP1A.190711.020/1617006012)",
        9: "Handheld", 10: "Vi India", 11: "WIFI",
        12: 1600, 13: 720, 14: "320",
        15: "ARM64 FP ASIMD AES | 2301 | 8", 16: 2799,
        17: "PowerVR Rogue GE8320",
        18: "OpenGL ES 3.2 build 1.11@5425693",
        19: "Google|9f7d6b8b-b10c-454a-852d-06332cd498eb",
        20: f"{random.randint(1,223)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        21: "en", 22: open_id, 23: str(platform),
        24: "Handheld", 25: "realme RMX2189", 26: CURRENT_REGION,
        29: access_token, 30: 1,
        41: "Vi India", 42: "WIFI",
        57: "1ac4b80ecf0478a44203bf8fac6120f5",
        60: 19799, 61: 2536, 62: 5056, 64: 2768,
        65: 19999, 66: 2536, 67: 19799,
        73: 1,
        74: "/data/app/com.dts.freefiremax-ShI7E0dK8p1IiZ785pvuVQ==/lib/arm64",
        76: 2,
        77: "38f4751a330688ab124c2c804cec90a5|/data/app/com.dts.freefiremax-ShI7E0dK8p1IiZ785pvuVQ==/base.apk",
        78: 2, 79: 2, 81: "64", 83: "2019118527",
        86: "OpenGLES3", 87: 3071, 88: 4, 92: 67920,
        93: "android_max",
        94: "KqsHT+UrR1HKqb6+1db+Ofei+NtZr2+hbiBo3yKDL8w+8E3S5qF2IgEEe1fFQFyHRzl4iyHjHp+QsfeLbjJ6+DidTiKxm0ak2uYYa6QR4nAUdlZR",
        95: 111107,
        96: '{"cur_rate":null,"support_etc2":true}',
        97: 1, 98: 1, 99: "4", 100: "4", 102: "",
        104: 83812, 105: 1,
        106: "https://dl-bs.ggpolarbear.com/live/ABHotUpdates/|https://core-bs.ggpolarbear.com/live/ABHotUpdates/|a4332cb1c1a84e51dd77441e4856ed5a",
        107: "1.9393e7b8e53e8aeb",
    }
    encrypted = aes_encrypt(assemble_proto(fields))
    headers = {
        "Host": "loginbp.ppmainecoonghj.com",
        "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
        "Accept": "*/*",
        "Accept-Encoding": "deflate, gzip",
        "X-GA-SV": str(int(time.time())),
        "Authorization": "Bearer",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB55",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Unity-Version": "2018.4.12f1",
        "Content-Length": str(len(encrypted)),
    }
    r = session.post(MAJOR_LOGIN_URL, data=encrypted, headers=headers,
                     timeout=30, verify=False)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    raw = r.content
    for buf in (aes_decrypt(raw), raw[64:] if len(raw) > 64 else b"", raw):
        if not buf:
            continue
        try:
            parsed = parse_proto(buf)
        except Exception:
            continue
        jwt = parsed.get(8)
        if isinstance(jwt, bytes):
            try:
                jwt = jwt.decode("utf-8", "ignore")
            except Exception:
                jwt = ""
        if isinstance(jwt, str) and jwt.startswith("eyJ") and jwt.count(".") == 2:
            sixteen = []
            for fnum in sorted(parsed.keys()):
                v = parsed[fnum]
                vals = v if isinstance(v, list) else [v]
                for val in vals:
                    if isinstance(val, (bytes, bytearray)) and len(val) == 16:
                        sixteen.append(bytes(val))
            k = sixteen[-2] if len(sixteen) >= 2 else AES_KEY
            i_ = sixteen[-1] if len(sixteen) >= 2 else AES_IV
            return jwt, k, i_
    raise RuntimeError("no JWT")


# ============================================================
# GENERATE ONE ACCOUNT
# ============================================================
def generate_one(idx, total, retries):
    spinner = Spinner(f"[{idx}/{total}] generating account ...")
    spinner.start()

    # Normal colors
    cyan = "\033[1;96m"
    green = "\033[1;92m"
    yellow = "\033[1;93m"
    white = "\033[1;97m"
    red = "\033[1;91m"
    reset = "\033[0m"

    try:
        session = requests.Session()
        session.verify = False
        password = hashlib.sha256(os.urandom(32)).hexdigest().upper()

        uid = retry(lambda: register_guest(session, password), retries=retries)
        if not uid:
            spinner.stop()
            log_err(f"[{idx}] register failed")
            return None

        res = retry(lambda: get_token(session, uid, password), retries=retries)
        if not res:
            spinner.stop()
            log_err(f"[{idx}] token failed for {uid}")
            return None

        access_token, open_id = res

        nickname = retry(
            lambda: generate_nickname(session, open_id),
            retries=retries
        )
        if not nickname:
            spinner.stop()
            log_err(f"[{idx}] nickname failed for {uid}")
            return None

        account_id = retry(
            lambda: send_major_register(
                session, nickname, access_token, open_id
            ),
            retries=retries
        )
        if not account_id:
            spinner.stop()
            log_err(f"[{idx}] major register failed for {uid}")
            return None

        retry(
            lambda: send_newbie(session, account_id, 3),
            retries=retries
        )

        login = retry(
            lambda: send_major_login(
                session, access_token, open_id
            ),
            retries=retries
        )
        if not login:
            spinner.stop()
            log_err(f"[{idx}] major login failed for {uid}")
            return None

        jwt, session_key, session_iv = login

        account = {
            "uid": uid,
            "password": password,
            "nickname": nickname,
            "open_id": open_id,
            "access_token": access_token,
            "account_id": account_id,
            "region": CURRENT_REGION,
            "jwt": jwt,
            "session_key": session_key.hex(),
            "session_iv": session_iv.hex(),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        spinner.stop()

        # Normal colored account box
        title = f"ACCOUNT GENERATED  [{idx}/{total}]"
        width = 60

        print()
        print(f"{cyan}╔{'═' * (width - 2)}╗{reset}")
        print(
            f"{cyan}║ {green}{title:<{width - 4}}{cyan} ║{reset}"
        )
        print(f"{cyan}╠{'═' * (width - 2)}╣{reset}")

        details = [
            ("UID", uid),
            ("Nickname", nickname),
            (
                "Open ID",
                open_id[:32] + "..." if len(open_id) > 32 else open_id
            ),
            ("Account ID", str(account_id)),
            ("Region", CURRENT_REGION),
            ("Password", password[:24] + "..."),
            (
                "JWT",
                jwt[:32] + "..." if len(jwt) > 32 else jwt
            ),
        ]

        for label, value in details:
            line = f"{label:<12}: {value}"
            line = line[:width - 4]
            print(f"{cyan}║ {yellow}{label:<12}{white}: {value:<{width - 17}}{cyan}║{reset}")

        print(f"{cyan}╚{'═' * (width - 2)}╝{reset}")
        print()

        return account

    except Exception as e:
        spinner.stop()
        log_err(f"[{idx}] unexpected error: {e}")
        return None


# ============================================================
# SERVER CHOOSER
# ============================================================
def choose_server():
    global CURRENT_REGION, CURRENT_HOST

    # Colors
    cyan = "\033[1;96m"
    blue = "\033[1;94m"
    green = "\033[1;92m"
    yellow = "\033[1;93m"
    red = "\033[1;91m"
    white = "\033[1;97m"
    reset = "\033[0m"

    print_server_box(SERVERS, 0)

    while True:
        try:
            raw = input(
                f"{cyan}  Enter choice number {white}(or type code){cyan}: {reset}"
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

        if not raw:
            continue

        if raw.isdigit():
            idx = int(raw) - 1

            if 0 <= idx < len(SERVERS):
                CURRENT_REGION = SERVERS[idx][0]
                CURRENT_HOST = SERVERS[idx][2]
                break

        code = raw.upper()

        for s in SERVERS:
            if s[0] == code:
                CURRENT_REGION = s[0]
                CURRENT_HOST = s[2]
                break
        else:
            print(f"{red}  ✗ Invalid choice, try again.{reset}")
            continue

        break

    print()
    print(f"{cyan}╔{'═' * 58}╗{reset}")
    print(
        f"{cyan}║  {green}✓ SELECTED SERVER:{reset} "
        f"{yellow}{CURRENT_REGION}{reset} "
        f"{white}({CURRENT_HOST})"
        f"{' ' * max(0, 58 - len(CURRENT_REGION) - len(CURRENT_HOST) - 22)}"
        f"{cyan}║{reset}"
    )
    print(f"{cyan}╚{'═' * 58}╝{reset}")
    print()

# ============================================================
# MAIN
# ============================================================
class ProgressBar:
    def __init__(self, total, prefix="Overall"):
        self.total = total
        self.prefix = prefix
        self.current = 0

        self.green = "\033[1;92m"
        self.reset = "\033[0m"

    def update(self, n=1):
        self.current += n

        percent = (self.current / self.total) * 100
        width = 40
        filled = int(width * self.current / self.total)

        bar = "█" * filled + "░" * (width - filled)

        print(
            f"\r{self.green}"
            f"{self.prefix} "
            f"[{bar}] "
            f"{percent:6.2f}% "
            f"({self.current}/{self.total})"
            f"{self.reset}",
            end="",
            flush=True
        )

    def finish(self):
        print()


def main():
    banner()
    load_proxies()
    choose_server()

    # Normal colors
    cyan = "\033[1;96m"
    green = "\033[1;92m"
    yellow = "\033[1;93m"
    red = "\033[1;91m"
    white = "\033[1;97m"
    blue = "\033[1;94m"
    reset = "\033[0m"

    try:
        total = int(
            input(
                f"{cyan}  Total accounts to generate "
                f"{white}[100]{cyan}: {reset}"
            ).strip() or "100"
        )
    except (ValueError, EOFError):
        total = 100

    try:
        threads = int(
            input(
                f"{cyan}  Generation threads (1-20) "
                f"{white}[10]{cyan}: {reset}"
            ).strip() or "10"
        )
    except (ValueError, EOFError):
        threads = 5

    try:
        retries = int(
            input(
                f"{cyan}  Retries per step (1-10) "
                f"{white}[3]{cyan}: {reset}"
            ).strip() or "3"
        )
    except (ValueError, EOFError):
        retries = 3

    threads = max(
        1,
        min(threads, 20 if _proxy_list else 3)
    )
    retries = max(1, min(retries, 10))

    print()
    print(f"{blue}{'═' * 60}{reset}")
    print(f"{cyan}  Region       : {yellow}{CURRENT_REGION}{reset}")
    print(f"{cyan}  Host         : {white}{CURRENT_HOST}{reset}")
    print(f"{cyan}  Total        : {white}{total}{reset}")
    print(f"{cyan}  Gen threads  : {white}{threads}{reset}")
    print(f"{cyan}  Retries      : {white}{retries}{reset}")
    print(f"{cyan}  Output       : {white}{OUTPUT_FILE}{reset}")
    print(f"{blue}{'═' * 60}{reset}")
    print()

    generated_ok = 0
    generated_fail = 0
    t0 = time.time()

    # Full Overall progress is green
    progress = ProgressBar(total, prefix="Overall")

    try:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [
                pool.submit(
                    generate_one,
                    i + 1,
                    total,
                    retries
                )
                for i in range(total)
            ]

            for f in as_completed(futures):
                try:
                    acc = f.result()
                except Exception as e:
                    acc = None
                    print(
                        f"\n{red}  ✗ Worker error: "
                        f"{e}{reset}"
                    )

                progress.update(1)

                if not acc:
                    generated_fail += 1
                    continue

                generated_ok += 1
                save_account(acc)

    except KeyboardInterrupt:
        print()
        print(
            f"{yellow}  ⚠ Interrupted by user{reset}"
        )

    progress.finish()

    elapsed = time.time() - t0

    print()
    print(f"{blue}{'═' * 60}{reset}")
    print(
        f"{green}  ✓ DONE in "
        f"{elapsed:.1f}s{reset}"
    )
    print(
        f"{green}  ✅ Generated : "
        f"{generated_ok}{reset}"
    )
    print(
        f"{red}  ❌ Failed    : "
        f"{generated_fail}{reset}"
    )
    print(
        f"{cyan}  📄 Saved to  : "
        f"{white}{OUTPUT_FILE}{reset}"
    )
    print(f"{blue}{'═' * 60}{reset}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print(RJB.paint("  Interrupted"))