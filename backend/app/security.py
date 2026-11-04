import base64, hashlib, hmac, os, re, secrets, time

from .bootstrap import load_env

load_env()
from collections import defaultdict, deque
import jwt
from cryptography.fernet import Fernet
from fastapi import HTTPException, Request

SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_urlsafe(48)      # random per run if unset (sessions reset on restart)
_enc_key = os.environ.get("TOKEN_ENC_KEY")
if not _enc_key:                                                            # derive a stable key from SECRET_KEY if not given
    _enc_key = base64.urlsafe_b64encode(hashlib.sha256(("tok:" + SECRET_KEY).encode()).digest()).decode()
FERNET = Fernet(_enc_key.encode())
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "false").lower() == "true"
SESSION_HOURS = 12


# ---- passwords (scrypt, stdlib) ----
def hash_password(pw: str) -> str:
    salt = os.urandom(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, n=2 ** 14, r=8, p=1)
    return "scrypt$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(h).decode()


def verify_password(pw: str, stored: str) -> bool:
    try:
        _, s, h = stored.split("$")
        calc = hashlib.scrypt(pw.encode(), salt=base64.b64decode(s), n=2 ** 14, r=8, p=1)
        return hmac.compare_digest(calc, base64.b64decode(h))
    except Exception:
        return False


# ---- sessions ----
def make_session(username: str) -> str:
    return jwt.encode({"sub": username, "exp": int(time.time()) + SESSION_HOURS * 3600}, SECRET_KEY, algorithm="HS256")


def read_session(token: str | None) -> str | None:
    if not token:
        return None
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])["sub"]
    except Exception:
        return None


# ---- agent / pairing keys ----
def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def new_secret(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


# ---- token encryption ----
def encrypt_token(t: str) -> str:
    return FERNET.encrypt(t.encode()).decode()


# ---- log scrubbing: never store tokens / credentials ----
_PATTERNS = [
    (re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b"), "[telegram-token-redacted]"),
    (re.compile(r"(?i)(password|passwd|pwd|secret|token|api[_-]?key)\s*[=:]\s*\S+"), r"\1=[redacted]"),
    (re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"), "[jwt-redacted]"),
]


def scrub(text: str) -> str:
    for rx, rep in _PATTERNS:
        text = rx.sub(rep, text)
    return text


# ---- rate limiting (in-memory sliding window; use a reverse proxy/redis if you run several workers) ----
_hits: dict[str, deque] = defaultdict(deque)


def rate_limit(bucket: str, limit: int, per: int = 60):
    def dep(request: Request):
        ip = request.client.host if request.client else "?"
        k = f"{bucket}:{ip}"
        now = time.time()
        q = _hits[k]
        while q and q[0] < now - per:
            q.popleft()
        if len(q) >= limit:
            raise HTTPException(429, "Too many requests")
        q.append(now)
    return dep
