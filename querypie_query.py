#!/usr/bin/env python3
"""QueryPie gRPC-Web 조회 클라이언트.

웹 UI 가 쓰는 것과 동일한 gRPC-Web(text) 인터페이스를 그대로 호출한다.
QueryPie 가 쿠키 인증이므로 브라우저 세션 쿠키를 재사용한다.
실행 가능한 SQL 범위는 QueryPie 계정 권한으로 서버측에서 통제된다
(DDL/DML 차단 여부는 이 스크립트가 아니라 계정 권한에 달려 있다).

설정과 자격은 모두 `~/.querypie/` 에 둔다 (QUERYPIE_HOME 으로 다른 곳을 지정할 수 있다).

서버 설정: querypie-config.json
  - `{"host": "querypie.example.com"}` — QueryPie 웹 UI 주소의 호스트만 적는다.
  - 기본값이 없으므로 이 파일이 없으면 그 자리에서 멈춘다.

로그인 credential: querypie-login.json
  - `{"username": "...", "password": "..."}` 형식으로 **사용자가 직접** 만든다.
  - 이 파일이 있으면 쿠키가 없거나 만료됐을 때 `authenticate` 로 자동 로그인해 쿠키를 갱신하므로,
    브라우저에서 쿠키를 복사하는 과정이 아예 필요 없다. `--login` 으로 강제 재로그인.
  - 값은 stdout 으로 출력하지 않는다.

쿠키 보관: querypie-cookie.json
  - `{"cookies": {"qp_access_token": "...", "qp_refresh_token": "..."}, "saved_at": "..."}`.
    `saved_at` 은 도구가 채우며, 선제 갱신이 실제로 돌고 있는지 그 값으로 본다.
  - 자동 로그인을 쓰면 이 파일은 도구가 알아서 만들고 갱신한다.
  - 이 스크립트는 쿠키 값을 stdout 으로 출력하지 않는다.

커넥션: 이름만 있으면 그 자리에서 조립한다 (저장해 둘 파일이 없다)
  - 이름은 클러스터 이름의 앞부분이면 된다 (`shop-cluster` 면 `shop`). 그 클러스터의 읽기
    엔드포인트를 골라 SessionService/open 페이로드를 만든다 — 노드를 박제하지 않으므로
    노드가 교체돼도 그대로 붙는다. 후보는 querypie_conn.py list 로 본다.
  - 복제 지연 없이 방금 바뀐 값을 봐야 하면 `--writer` 로 쓰기 엔드포인트에 붙는다
    (읽기 엔드포인트가 없는 클러스터는 어느 쪽이든 같은 곳으로 간다).
  - 어느 이름을 쓸지는 querypie-conn-map.json (database -> 커넥션 이름) 이 정한다.
    매핑에 없으면 --conn-name 으로 직접 지정한다 (갱신은 querypie_conn.py map).
  - 조립이 안 되는 커넥션만 querypie-open-<이름>.b64 로 폴백한다 (querypie_conn.py add).
  - 세션의 current database 는 서버가 정한 값으로 고정된다 (--db 는 커넥션 선택과 요청의
    databaseName 필드에만 쓰인다). 그래서 SQL 에서는 `db.table` 로 스키마를 한정할 것 —
    한정하지 않으면 엉뚱한 기본 DB 에서 찾다가 SqlResultsetNotFound 가 된다.

사용 예:
  # database 매핑으로 커넥션 자동 선택
  python querypie_query.py --db shop --sql "SELECT 1" --rows 50

  # 매핑에 없는 database 는 커넥션을 직접 지정
  python querypie_query.py --conn-name archive --db shop_log --sql "SELECT 1"

  # 값을 직접 지정
  python querypie_query.py --conn <uuid> --db shop --sql-file q.sql --rows 500

  # 응답 원형(raw protobuf) 구조 확인
  python querypie_query.py --db shop --sql "SELECT 1" --dump

  # 값을 자르지 않고 백업 (긴 JSON 컬럼 등). 진행 메시지는 stderr 로 나가 파일엔 데이터만 담긴다
  python querypie_query.py --db shop --sql-file backup.sql --rows 1000 --tsv > backup.tsv

긴 값(LOB) 다루기:
  - 서버는 긴 셀을 핸들 `{"id":"<uuid>","preview":"앞부분...","type":"CLOB"}` 로 내려준다.
    이 도구는 largeObjectView 로 전체 값을 받아 채우므로 EXPLAIN FORMAT=JSON 이나 긴
    TEXT/JSON 컬럼도 그대로 나온다 (셀마다 왕복이 한 번 더 드니 --lob-cells 로 상한을 둔다).
  - 백업처럼 값이 잘리면 안 되는 경우에는 `--tsv` 를 함께 쓸 것 — 표 모드는 컬럼폭
    상한(--max-col-width, 기본 80)에서 말줄임한다.

행 수 다루기:
  - `--rows` 하나만 주면 된다. 서버가 결과셋을 만들 때 자르는 값(execute 의 limitCount)도
    같은 값으로 맞추므로, 큰 수를 줘도 중간에서 잘리지 않는다.
  - 결과가 그 상한을 **정확히** 채우면 뒤가 더 있는지 알 수 없다 — 서버는 잘렸다고 알려주지
    않으므로 도구가 경고를 남긴다. 그때는 `--rows` 를 늘리거나 `--start-row` 로 이어 받는다.
"""
import argparse
import base64
import http.client
import json
import os
import re
import secrets
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# 설정과 자격은 모두 이 디렉터리에 둔다. 다른 곳을 쓰려면 QUERYPIE_HOME 으로 지정한다.
HOME = os.environ.get("QUERYPIE_HOME") or os.path.join(os.path.expanduser("~"), ".querypie")
# 서버 주소처럼 환경마다 다른 값 — 사용자가 직접 작성한다
CONFIG_FILE = os.path.join(HOME, "querypie-config.json")
COOKIE_FILE = os.path.join(HOME, "querypie-cookie.json")
CONN_FILE = os.path.join(HOME, "querypie-connection.txt")
# 단일 커넥션 시절 경로 (커넥션 이름을 못 정했을 때의 fallback)
OPEN_FILE = os.path.join(HOME, "querypie-open.b64")
# RDS 인스턴스마다 QueryPie 커넥션이 달라 커넥션별로 나눠 보관한다
OPEN_FILE_TMPL = os.path.join(HOME, "querypie-open-{name}.b64")
# database 이름 -> 커넥션 이름 매핑 (SHOW DATABASES 결과로 채운다)
CONN_MAP_FILE = os.path.join(HOME, "querypie-conn-map.json")
# 로그인 credential {"username": ..., "password": ...} — 사용자가 직접 작성한다
LOGIN_FILE = os.path.join(HOME, "querypie-login.json")

_CONFIG = None


def config():
    """querypie-config.json 을 한 번만 읽어 돌려준다.

    서버 주소는 쓰는 곳마다 다르므로 기본값을 두지 않는다 — 없으면 그 자리에서 멈춰,
    엉뚱한 곳으로 요청이 나가거나 한참 뒤에 인증 오류로 드러나는 일이 없게 한다.
    """
    global _CONFIG
    if _CONFIG is None:
        if not os.path.exists(CONFIG_FILE):
            sys.exit(f"설정 파일이 없습니다: {CONFIG_FILE}\n"
                     '  {"host": "querypie.example.com"} 형태로 만드세요 '
                     "(QueryPie 웹 UI 주소에서 호스트만 적습니다).")
        with open(CONFIG_FILE, encoding="utf-8") as fp:
            try:
                _CONFIG = json.load(fp)
            except ValueError as e:
                sys.exit(f"설정 파일을 읽지 못했습니다({e}): {CONFIG_FILE}")
    return _CONFIG


def host():
    """QueryPie 서버 호스트 (`querypie.example.com` 처럼 스킴 없이)."""
    value = config().get("host")
    if not value:
        sys.exit(f'{CONFIG_FILE} 에 "host" 가 없습니다.')
    return value


def ensure_home():
    """자격이나 캐시를 쓰기 전에 디렉터리를 만들어 둔다."""
    os.makedirs(HOME, exist_ok=True)

SVC_SQL = "engine.sql.SQLService"
SVC_CONN = "common.connection.ConnectionService"

# TSV 출력 모드 여부. 진행 메시지를 stderr 로 돌려 stdout 이 데이터 전용이 되게 한다.
_TSV = False


def info(*args):
    """진행, 상태 메시지 출력. TSV 모드에서는 stdout(데이터) 오염을 막기 위해 stderr 로 보낸다."""
    print(*args, file=sys.stderr if _TSV else sys.stdout)


# --------------------------------------------------------------------------
# protobuf 인코딩/디코딩 (필요한 최소 기능만)
# --------------------------------------------------------------------------
def vi(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def tag(field, wire):
    return vi((field << 3) | wire)


def f_str(field, text):
    if not text:
        return b""
    raw = text.encode("utf-8")
    return tag(field, 2) + vi(len(raw)) + raw


def f_msg(field, payload):
    if not payload:
        return b""
    return tag(field, 2) + vi(len(payload)) + payload


def f_vi(field, n):
    if not n:
        return b""
    return tag(field, 0) + vi(n)


def read_vi(b, i):
    shift = val = 0
    while True:
        x = b[i]
        i += 1
        val |= (x & 0x7F) << shift
        if not x & 0x80:
            return val, i
        shift += 7


def decode_raw(b, depth=0):
    """스키마 없이 protobuf 구조를 추정 디코딩 (탐색/디버깅용)."""
    out, i = [], 0
    while i < len(b):
        try:
            key, i = read_vi(b, i)
        except IndexError:
            break
        fn, wt = key >> 3, key & 7
        if fn == 0:
            break
        if wt == 0:
            try:
                v, i = read_vi(b, i)
            except IndexError:
                break
            out.append((fn, "varint", v))
        elif wt == 2:
            try:
                ln, i = read_vi(b, i)
            except IndexError:
                break
            sub, i = b[i:i + ln], i + ln
            if len(sub) != ln:
                break
            txt = None
            try:
                cand = sub.decode("utf-8")
                if cand.isprintable() or "\n" in cand:
                    txt = cand
            except UnicodeDecodeError:
                pass
            nested = decode_raw(sub, depth + 1) if (depth < 5 and txt is None and sub) else None
            out.append((fn, "str" if txt is not None else ("msg" if nested else "bytes"),
                        txt if txt is not None else (nested if nested else sub)))
        elif wt == 5:
            i += 4
            out.append((fn, "fixed32", None))
        elif wt == 1:
            i += 8
            out.append((fn, "fixed64", None))
        else:
            break
    return out


def find_field(items, fn, kind=None):
    for f, k, v in items:
        if f == fn and (kind is None or k == kind):
            return v
    return None


def extract_field_raw(b, target_fn):
    """protobuf 메시지 b 에서 필드번호 target_fn 의 length-delimited raw 값을 그대로 반환."""
    i = 0
    while i < len(b):
        try:
            key, i = read_vi(b, i)
        except IndexError:
            break
        fn, wt = key >> 3, key & 7
        if fn == 0:
            break
        if wt == 2:
            ln, i = read_vi(b, i)
            if fn == target_fn:
                return b[i:i + ln]
            i += ln
        elif wt == 0:
            _, i = read_vi(b, i)
        elif wt == 5:
            i += 4
        elif wt == 1:
            i += 8
        else:
            break
    return None


def mp_read(b, i):
    """QueryPie 행 blob 용 msgpack 디코더 (필요한 타입만)."""
    c = b[i]; i += 1
    if c <= 0x7f:
        return c, i
    if c >= 0xe0:
        return c - 256, i
    if 0x80 <= c <= 0x8f:
        d = {}
        for _ in range(c & 0x0f):
            k, i = mp_read(b, i); v, i = mp_read(b, i); d[k] = v
        return d, i
    if 0x90 <= c <= 0x9f:
        a = []
        for _ in range(c & 0x0f):
            v, i = mp_read(b, i); a.append(v)
        return a, i
    if 0xa0 <= c <= 0xbf:
        n = c & 0x1f
        return b[i:i + n].decode("utf-8", "replace"), i + n
    simple = {0xc0: None, 0xc2: False, 0xc3: True}
    if c in simple:
        return simple[c], i
    width = {0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8}
    if c in width:
        n = width[c]; return int.from_bytes(b[i:i + n], "big"), i + n
    swidth = {0xd0: 1, 0xd1: 2, 0xd2: 4, 0xd3: 8}
    if c in swidth:
        n = swidth[c]; return int.from_bytes(b[i:i + n], "big", signed=True), i + n
    strw = {0xd9: 1, 0xda: 2, 0xdb: 4}
    if c in strw:
        w = strw[c]; n = int.from_bytes(b[i:i + w], "big"); i += w
        return b[i:i + n].decode("utf-8", "replace"), i + n
    if c in (0xdc, 0xdd):
        w = 2 if c == 0xdc else 4; n = int.from_bytes(b[i:i + w], "big"); i += w
        a = []
        for _ in range(n):
            v, i = mp_read(b, i); a.append(v)
        return a, i
    if c in (0xde, 0xdf):
        w = 2 if c == 0xde else 4; n = int.from_bytes(b[i:i + w], "big"); i += w
        d = {}
        for _ in range(n):
            k, i = mp_read(b, i); v, i = mp_read(b, i); d[k] = v
        return d, i
    if c == 0xca:
        import struct; return struct.unpack(">f", b[i:i + 4])[0], i + 4
    if c == 0xcb:
        import struct; return struct.unpack(">d", b[i:i + 8])[0], i + 8
    raise ValueError(f"지원하지 않는 msgpack 바이트 0x{c:02x} @ {i - 1}")


def iter_fields(b):
    """protobuf 메시지에서 (fieldnum, raw_value) 를 순서대로 yield (length-delimited 만)."""
    i = 0
    while i < len(b):
        try:
            key, i = read_vi(b, i)
        except IndexError:
            break
        fn, wt = key >> 3, key & 7
        if fn == 0:
            break
        if wt == 2:
            ln, i = read_vi(b, i); yield fn, b[i:i + ln]; i += ln
        elif wt == 0:
            _, i = read_vi(b, i)
        elif wt == 5:
            i += 4
        elif wt == 1:
            i += 8
        else:
            break


def render_data_table(frames, max_col_width=80, tsv=False, expand=None):
    """getDataTable 응답 프레임 → 컬럼 헤더 + 행 값 표로 출력.

    컬럼폭은 헤더명과 그 컬럼의 모든 값 길이 중 최댓값으로 잡아 정렬을 맞춘다
    (max_col_width 로 상한을 둬 긴 값이 표를 과도하게 넓히지 않게 한다).

    tsv=True 면 정렬/말줄임 없이 탭 구분으로 출력한다 - 값이 잘리면 안 되는
    백업 용도(긴 JSON 컬럼 등)에 쓰고, 리다이렉트로 파일에 그대로 담을 수 있다.

    expand 는 파싱된 행 목록을 받아 돌려주는 후처리다 (LOB 핸들 복원에 쓴다).

    출력한 행 수를 돌려준다 — 호출자가 상한에 닿았는지(= 잘렸을 수 있는지) 판정한다.
    """
    cols, rows_blob = [], None
    for d in frames:
        body = extract_field_raw(d, 2)
        if not body:
            continue
        for fn, val in iter_fields(body):
            if fn == 4:  # 컬럼 정의 {#2 name, #3 clrType}
                nm = find_field(decode_raw(val), 2, "str")
                if nm is not None:
                    cols.append(nm)
            elif fn == 5:  # 행 msgpack blob
                rows_blob = val
    if not cols:
        info("  (컬럼 없음)")
        return 0

    # 행 먼저 파싱 (컬럼폭 계산에 값 길이가 필요하므로)
    parsed = []
    if rows_blob is not None:
        try:
            rows, _ = mp_read(rows_blob, 0)
        except Exception as exc:
            print(f"  (행 디코드 실패: {exc})")
            rows = None
        for row in (rows if isinstance(rows, list) else []):
            cells = row.get("v", []) if isinstance(row, dict) else []
            out = []
            for cell in cells:
                if isinstance(cell, dict):
                    out.append("NULL" if cell.get("n") else str(cell.get("v", "")))
                else:
                    out.append(str(cell))
            out += [""] * (len(cols) - len(out))
            parsed.append(out[:len(cols)])

    if expand and parsed:
        # 복원된 BLOB 은 bytes 라 표/TSV 조립에서 str 과 섞이면 깨진다
        parsed = [[v if isinstance(v, str) else str(v) for v in row]
                  for row in expand(parsed)]

    if tsv:
        # 값 안의 탭/개행은 열, 행 구분을 깨뜨리므로 치환한다 (백업 복원 시 역치환 불필요 -
        # JSON 컬럼에는 실개행이 없고, 있더라도 \\n 표기가 JSON 문법상 동등하다).
        def tsv_cell(v):
            return v.replace("\t", " ").replace("\r", "").replace("\n", "\\n")

        print("\t".join(cols))
        if rows_blob is None:
            info("  (행 데이터 없음)")
            return 0
        for out in parsed:
            print("\t".join(tsv_cell(v) for v in out))
        return len(parsed)

    # 표는 한 행이 한 줄이라 값 안의 개행/탭을 표기로 바꾼다 (복원한 LOB 은 여러 줄이다)
    parsed = [[v.replace("\t", " ").replace("\r", "").replace("\n", "\\n") for v in row]
              for row in parsed]

    # 컬럼폭 = max(헤더명, 해당 컬럼 값들), 상한 max_col_width
    widths = [len(c) for c in cols]
    for out in parsed:
        for i, v in enumerate(out):
            widths[i] = max(widths[i], len(v))
    widths = [min(max(3, w), max_col_width) for w in widths]

    def cell(v, w):
        v = v if len(v) <= w else v[:w - 1] + "…"  # 넘치면 말줄임
        return v.ljust(w)

    print("  " + " | ".join(cell(c, w) for c, w in zip(cols, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    if rows_blob is None:
        info("  (행 데이터 없음)")
        return 0
    for out in parsed:
        print("  " + " | ".join(cell(v, w) for v, w in zip(out, widths)))
    return len(parsed)


def show(items, indent=2):
    for fn, kind, val in items:
        pad = " " * indent
        if kind == "msg":
            print(f"{pad}#{fn} message")
            show(val, indent + 2)
        elif kind == "str":
            v = val if len(val) <= 300 else val[:300] + " ...(truncated)"
            print(f"{pad}#{fn} str({len(val)}) {v!r}")
        elif kind == "bytes":
            print(f"{pad}#{fn} bytes({len(val)})")
        else:
            print(f"{pad}#{fn} {kind} {val}")


# --------------------------------------------------------------------------
# gRPC-Web(text) 전송
# --------------------------------------------------------------------------
B64SEG = re.compile(rb"[A-Za-z0-9+/]+={0,2}")


def frame(payload):
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


# TLS 핸드셰이크를 매 요청마다 반복하면 요청당 약 0.05초가 더 든다. 조회 한 번에도
# parse/execute/getDataTable 3회를 왕복하고, 프록시는 이 경로를 계속 쓰므로
# 커넥션을 살려 재사용한다. 끊겼으면 다시 연결해 1회 재시도한다.
# 커넥션은 스레드마다 따로 둔다. 하나를 락으로 공유하면 요청이 직렬화되어
# 여러 조회를 동시에 보낼 수 없다 (프록시가 테이블 속성을 병렬로 받아올 때 필요).
_LOCAL = threading.local()


def http_post(path, body, headers, insecure=False, timeout=180):
    """POST 후 (status, reason, 헤더 리스트, 응답 바디). 커넥션은 스레드별로 재사용한다."""
    conns = getattr(_LOCAL, "conns", None)
    if conns is None:
        conns = _LOCAL.conns = {}
    for attempt in (1, 2):
        conn = conns.get(insecure)
        if conn is None:
            ctx = ssl._create_unverified_context() if insecure else ssl.create_default_context()
            conn = http.client.HTTPSConnection(host(), timeout=timeout, context=ctx)
            conns[insecure] = conn
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            return resp.status, resp.reason, resp.getheaders(), resp.read()
        except (http.client.HTTPException, OSError):
            try:
                conn.close()
            except Exception:
                pass
            conns[insecure] = None
            if attempt == 2:
                raise


def unframe(raw):
    out, i = [], 0
    while i + 5 <= len(raw):
        flag = raw[i]
        ln = int.from_bytes(raw[i + 1:i + 5], "big")
        out.append((flag, raw[i + 5:i + 5 + ln]))
        i += 5 + ln
    return out


def b64d(text):
    if isinstance(text, str):
        text = text.encode("ascii", "ignore")
    text = b"".join(text.split())
    try:
        return base64.b64decode(text + b"=" * (-len(text) % 4))
    except Exception:
        out = bytearray()
        for seg in B64SEG.findall(text):
            try:
                out += base64.b64decode(seg + b"=" * (-len(seg) % 4))
            except Exception:
                continue
        return bytes(out)


def load_cookies():
    """저장된 쿠키를 {이름: 값} 으로 로드 (없으면 빈 dict).

    파일은 `{"cookies": {...}, "saved_at": "..."}` 형태다. 값은 화면에 출력하지 않는다.
    """
    if not os.path.exists(COOKIE_FILE):
        return {}
    with open(COOKIE_FILE, encoding="utf-8") as fp:
        try:
            data = json.load(fp)
        except ValueError as e:
            sys.exit(f"쿠키 파일을 읽지 못했습니다({e}): {COOKIE_FILE}")
    cookies = data.get("cookies") if isinstance(data, dict) else None
    return cookies if isinstance(cookies, dict) else {}


def load_cookie():
    """Cookie 헤더에 그대로 넣을 문자열."""
    cookies = load_cookies()
    if not cookies:
        sys.exit(
            f"쿠키가 없습니다: {COOKIE_FILE}\n"
            f'  {{"cookies": {{"qp_access_token": "...", "qp_refresh_token": "..."}}}} 형태로 저장하세요.\n'
            "  값은 DevTools > Network > engine-grpc 요청 > Request Headers 의 Cookie 에 있고,\n"
            "  화면에는 출력되지 않습니다. (login 파일이 있으면 자동 로그인이 알아서 만듭니다)"
        )
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


def has_cookies():
    """쓸 수 있는 쿠키가 있는지."""
    return bool(load_cookies())


def save_cookies(cookies):
    """쿠키를 JSON 으로 저장한다. `saved_at` 은 갱신이 실제로 돌고 있는지 보는 용도다."""
    payload = {"cookies": cookies, "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    ensure_home()
    os.umask(0o077)
    with open(COOKIE_FILE, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _update_cookie_file(access=None, refresh=None):
    """RefreshToken/authenticate 응답으로 쿠키 파일의 qp_access_token / qp_refresh_token 을 갱신.
    (rotation 이므로 새 refresh token 도 반드시 저장해야 다음 refresh 가 된다.)
    파일이 없으면 새로 만든다 — 로그인으로 처음 쿠키를 얻는 경로."""
    d = load_cookies()
    if access is not None:
        d["qp_access_token"] = access
    if refresh is not None:
        d["qp_refresh_token"] = refresh
    save_cookies(d)


def load_connection():
    """저장된 connectionUuid (없으면 빈 문자열).

    보통은 세션 페이로드 #2 에서 자동 추출되므로 필요 없다. 페이로드 없이 쓰려면
    querypie-connection.txt 에 connectionUuid 한 줄을 저장한다."""
    if not os.path.exists(CONN_FILE):
        return ""
    with open(CONN_FILE, encoding="utf-8") as fp:
        return fp.read().strip()


def resolve_conn_name(db, explicit=None):
    """조회 대상 database 에 맞는 커넥션 이름을 결정 (못 찾으면 None).

    우선순위: --conn-name 명시 > querypie-conn-map.json 의 database 매핑 >
    database 이름 그대로 (그 이름으로 시작하는 클러스터가 있으면 그대로 잡힌다).
    """
    if explicit:
        return explicit
    if not db:
        return None
    if os.path.exists(CONN_MAP_FILE):
        with open(CONN_MAP_FILE, encoding="utf-8") as fp:
            name = json.load(fp).get(db)
        if name:
            return name
    return db


def load_open(name=None):
    """저장해 둔 SessionService/open 페이로드를 (bytes, 파일명) 으로 로드 (없으면 (None, None)).

    예전에는 이 파일이 유일한 경로였지만 지금은 폴백이다 — 커넥션은 이름만으로
    그 자리에서 조립한다(resolve_open_payload). 파일은 특정 노드를 박제해 둔 것이라
    그 노드가 교체되면 죽으므로, 조립이 안 되는 커넥션에만 쓴다.
    """
    paths = []
    if name:
        paths.append(OPEN_FILE_TMPL.format(name=name))
    paths.append(OPEN_FILE)
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as fp:
            txt = fp.read().strip()
        if txt:
            return base64.b64decode(txt), os.path.basename(path)
    return None, None


# --------------------------------------------------------------------------
# 커넥션 해석 — 이름 하나로 클러스터 엔드포인트 세션을 연다
# --------------------------------------------------------------------------
# open 페이로드의 Connection 객체(#6)는 getConnection 응답을 재배치한 것이다.
# 왼쪽이 open, 오른쪽이 getConnection 의 자리다 (아래는 엔드포인트 기준. 노드는 몇 자리가
# 다르며 그 차이는 build_open_payload 에 적어 두었다).
#   #6.2 = gc#2.3(커넥션 uuid)   #6.3 = gc#2.4(host)      #6.4 = gc#2.6(port)
#   #6.9 = gc#1.4(클러스터 이름)  #6.17 = gc#2.2(인스턴스 종류)
#   #6.18 = gc#2.3               #6.19 = gc#1.2(클러스터 종류)
#   #6.20 = gc#1.3(클러스터 uuid) #6.21 = gc#17(권한 객체)  #6.51 = gc#8.1(DB 계정)
#   #6.52 = gc#10.2(리전)        #6.1 = #6.7 = #6.55 = 1   #6.58 = 빈 문자열
# 최상위는 #1=2, #2=커넥션 uuid, #6=위 객체, #12=클라이언트가 만드는 sessionId,
# #22=커넥션 uuid, #23=커넥션 종류다.
#
# 그 종류(#23)는 getConnection 의 #1 과 같은 값이라, 엔드포인트를 4 로 물으면
# `Cluster NotFound`, 노드를 3 으로 물으면 같은 오류가 난다 (2026-09-17 실측).
OPEN_CONNECTION_TYPE = 2
OBJECT_TYPE_CLUSTER = 3   # 클러스터 엔드포인트
OBJECT_TYPE_INSTANCE = 4  # 클러스터 아래 개별 노드
KIND_ENDPOINT = "엔드포인트"
KIND_NODE = "노드"
KIND_TO_OBJECT_TYPE = {KIND_ENDPOINT: OBJECT_TYPE_CLUSTER, KIND_NODE: OBJECT_TYPE_INSTANCE}


def new_uuid():
    h = secrets.token_hex(16)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def build_open_payload(gc_frame, session_id=None, object_type=OBJECT_TYPE_CLUSTER):
    """getConnection 응답 프레임 → SessionService/open 요청 페이로드.

    노드(OBJECT_TYPE_INSTANCE)는 응답 구조가 달라, 대상이 #2 가 아니라 #3 에 오고 host 도
    이름(#4) 대신 DB 계정(#8.3)에 담긴다. #17/#18 은 두 경우 모두 엔드포인트(#2)의 값이라
    노드 페이로드에는 그 노드가 아니라 부모 엔드포인트의 식별자가 들어간다 (2026-09-17 실측).
    """
    cluster = extract_field_raw(gc_frame, 1)
    endpoint = extract_field_raw(gc_frame, 2)
    node = extract_field_raw(gc_frame, 3)
    db_user = extract_field_raw(gc_frame, 8)
    region = extract_field_raw(gc_frame, 10)
    permission = extract_field_raw(gc_frame, 17)
    if not (cluster and endpoint and db_user and region and permission):
        raise ValueError("getConnection 응답에서 필요한 필드를 찾지 못했습니다")

    ci, ei, ui = decode_raw(cluster), decode_raw(endpoint), decode_raw(db_user)
    endpoint_uuid = find_field(ei, 3, "str")
    if object_type == OBJECT_TYPE_INSTANCE:
        if not node:
            raise ValueError("노드 커넥션인데 응답에 노드 정보(#3)가 없습니다")
        conn_uuid = find_field(decode_raw(node), 3, "str")
        host = find_field(ui, 3, "str")
    else:
        conn_uuid = endpoint_uuid
        host = find_field(ei, 4, "str")
    port = find_field(ei, 6, "varint") or 3306
    instance_type = find_field(ei, 2, "varint")
    cluster_uuid = find_field(ci, 3, "str")
    cluster_name = find_field(ci, 4, "str")
    cluster_type = find_field(ci, 2, "varint")
    region_name = find_field(decode_raw(region), 2, "str") or ""
    if not (conn_uuid and host and cluster_uuid):
        raise ValueError("커넥션/호스트/클러스터 식별자를 찾지 못했습니다")

    connection = (
        f_vi(1, 1)
        + f_str(2, conn_uuid)
        + f_str(3, host)
        + f_vi(4, port)
        + f_vi(7, 1)
        + f_str(9, cluster_name or "")
        + f_vi(17, instance_type)
        + f_str(18, endpoint_uuid)
        + f_vi(19, cluster_type)
        + f_str(20, cluster_uuid)
        + f_msg(21, permission)
        + f_msg(51, extract_field_raw(db_user, 1))
        + f_str(52, region_name)
        + f_vi(55, 1)
        # f_str 은 빈 문자열을 통째로 생략하므로 길이 0 필드를 직접 만든다
        + tag(58, 2) + vi(0)
    )
    return (f_vi(1, OPEN_CONNECTION_TYPE)
            + f_str(2, conn_uuid)
            + f_msg(6, connection)
            + f_str(12, session_id or new_uuid())
            + f_str(22, conn_uuid)
            + f_vi(23, object_type))


def list_clusters(insecure=False, window_id=""):
    """접근 가능한 클러스터 → [(그룹명, uuid, 이름)]. 사용자 식별자는 필요 없다."""
    out = []
    for d in call(SVC_CONN, "getUserClusterGroups", b"", insecure, window_id=window_id):
        for fn, group in iter_fields(d):
            if fn != 1:
                continue
            meta = extract_field_raw(group, 1)
            group_name = find_field(decode_raw(meta), 4, "str") if meta else None
            for fn2, wrapper in iter_fields(group):
                if fn2 != 2:
                    continue
                inner = extract_field_raw(wrapper, 1) or wrapper
                items = decode_raw(inner)
                uuid, name = find_field(items, 3, "str"), find_field(items, 4, "str")
                if uuid and name:
                    out.append((group_name, uuid, name))
    return out


def list_instances(cluster_uuid, insecure=False, window_id=""):
    """클러스터의 커넥션 → [(커넥션 uuid, 이름, 종류)].

    UI 와 마찬가지로 두 레벨이 모두 커넥션이다 — 클러스터 엔드포인트(응답 #1.#1)와
    그 아래 개별 노드(#1.#2.#1). 어느 쪽을 쓸지는 pick_instance 가 정한다.
    """
    out = []
    payload = f_str(2, cluster_uuid)
    for d in call(SVC_CONN, "getUserInstances", payload, insecure, window_id=window_id):
        for fn, wrapper in iter_fields(d):
            if fn != 1:
                continue
            endpoint = extract_field_raw(wrapper, 1)
            if endpoint:
                items = decode_raw(endpoint)
                uuid, name = find_field(items, 3, "str"), find_field(items, 4, "str")
                if uuid and name:
                    out.append((uuid, name, KIND_ENDPOINT))
            for fn2, node_wrapper in iter_fields(wrapper):
                if fn2 != 2:
                    continue
                node = extract_field_raw(node_wrapper, 1)
                if not node:
                    continue
                items = decode_raw(node)
                uuid, name = find_field(items, 3, "str"), find_field(items, 4, "str")
                if uuid and name:
                    out.append((uuid, name, KIND_NODE))
    return out


def pick_instance(rows, prefer_writer=False):
    """클러스터의 커넥션 중 하나를 고른다. 엔드포인트가 먼저이고 노드는 마지막이다.

    노드를 콕 집으면 그 노드가 교체될 때 커넥션이 통째로 사라지는데, 클러스터
    엔드포인트는 그 교체를 RDS 가 흡수한다. 엔드포인트가 둘이면 기본은 읽기(-ro-) 쪽이다 —
    조회에 쓰기 인스턴스의 부하를 얹을 이유가 없다. 쓰기 문장을 실을 때(승인 요청,
    --allow-write 프록시)와 방금 바꾼 값을 복제 지연 없이 봐야 할 때만 prefer_writer 를 준다.
    """
    if not rows:
        return None
    endpoints = [r for r in rows if r[2] == KIND_ENDPOINT]
    picked = [r for r in endpoints if ("-ro-" in r[1]) != prefer_writer]
    return (picked or endpoints or rows)[0]


def open_payload_for(pick, insecure=False, window_id=""):
    """(커넥션 uuid, 이름, 종류) → open 페이로드. 종류에 따라 조회 kind 가 갈린다."""
    object_type = KIND_TO_OBJECT_TYPE.get(pick[2], OBJECT_TYPE_CLUSTER)
    frames = get_connection(pick[0], kind=object_type, insecure=insecure, window_id=window_id)
    return build_open_payload(frames[0], object_type=object_type)


def find_cluster(name, insecure=False, window_id=""):
    """커넥션 이름 → (클러스터 uuid, 클러스터 이름). 못 찾거나 모호하면 ValueError."""
    clusters = list_clusters(insecure, window_id)
    low = name.lower()
    for rule in (lambda n: n == low,
                 lambda n: n == f"{low}-cluster",
                 lambda n: n.startswith(low),
                 lambda n: low in n):
        hits = [c for c in clusters if rule(c[2].lower())]
        if len(hits) == 1:
            return hits[0][1], hits[0][2]
        if len(hits) > 1:
            raise ValueError(f"'{name}' 에 맞는 클러스터가 여럿입니다: "
                             + ", ".join(c[2] for c in hits))
    raise ValueError(f"'{name}' 에 맞는 클러스터가 없습니다")


def resolve_open_payload(name, insecure=False, window_id="", allow_file=True,
                         prefer_writer=False):
    """커넥션 이름 → (open 페이로드, 출처 설명). 못 만들면 (None, None).

    클러스터 엔드포인트를 그 자리에서 조립하므로 저장해 둘 것이 없다. 조립이 안 되는
    커넥션만 저장된 b64 파일로 넘어간다(allow_file=False 면 조립만 시도한다 — 조립과
    폴백을 구분해야 하는 호출자를 위한 것이다). 커넥션은 RDS 클러스터 단위이므로 그
    클러스터에 없는 database 를 지정하면 서버가 SqlResultsetNotFound 로 응답한다.
    """
    if name:
        try:
            cluster_uuid, cluster_name = find_cluster(name, insecure, window_id)
            pick = pick_instance(list_instances(cluster_uuid, insecure, window_id),
                                 prefer_writer)
            if pick is None:
                raise ValueError(f"클러스터 {cluster_name} 에 커넥션이 없습니다")
            # 역할은 고른 결과를 그대로 보여준다 — 읽기 엔드포인트가 없는 클러스터도 있다
            role = ("읽기 " if "-ro-" in pick[1] else "쓰기 ") if pick[2] == KIND_ENDPOINT else ""
            return (open_payload_for(pick, insecure, window_id),
                    f"{cluster_name} {role}{pick[2]} ({name})")
        except (ValueError, SystemExit) as e:
            if allow_file:
                info(f"[open] 커넥션 조립 실패({name}): {e}")
    if not allow_file:
        return None, None
    payload, src = load_open(name)
    return (payload, f"파일 {src}") if payload else (None, None)


def call(service, method, payload, insecure=False, timeout=180, window_id="", _allow_refresh=True):
    headers = {
        "content-type": "application/grpc-web-text",
        "accept": "application/grpc-web-text",
        "x-grpc-web": "1",
        "x-user-agent": "grpc-web-javascript/0.1",
        "cookie": load_cookie(),
    }
    if window_id:
        headers["x-querypie-window-id"] = window_id
    try:
        status_code, reason, hdr_list, body = http_post(
            f"/engine-grpc/{service}/{method}",
            base64.b64encode(frame(payload)), headers, insecure, timeout)
    except (http.client.HTTPException, OSError) as e:
        sys.exit(f"연결 실패: {e} (VPN 연결 상태를 확인하세요)")
    if status_code >= 400:
        sys.exit(f"HTTP {status_code} {reason} — 쿠키 만료 또는 권한 문제일 수 있습니다.")
    raw = b64d(body)
    hdrs = {k.lower(): v for k, v in hdr_list}

    status = hdrs.get("grpc-status")
    message = hdrs.get("grpc-message")
    data = []
    for flag, payload_ in unframe(raw):
        if flag & 0x80:
            txt = payload_.decode("utf-8", "replace")
            m = re.search(r"grpc-status:\s*(\d+)", txt)
            if m:
                status = m.group(1)
            m = re.search(r"grpc-message:\s*(.+)", txt)
            if m:
                message = m.group(1).strip()
        else:
            data.append(payload_)
    if status not in (None, "0"):
        # access token 만료류 인증 오류면 RefreshToken 으로 갱신 후 1회 재시도
        if _allow_refresh and _is_auth_error(status, message):
            if _refresh_tokens(insecure, window_id):
                info("[refresh] access token 갱신 후 재시도")
                return call(service, method, payload, insecure, timeout, window_id, _allow_refresh=False)
        sys.exit(f"QueryPie 오류: {humanize_grpc(message)} "
                 f"(status={status}, {service}/{method})")
    return data


def humanize_grpc(message):
    """grpc-message(base64 로 감싼 protobuf)에서 사람이 읽을 오류 문구를 뽑는다.

    예: 'CMLqARIWChRTcWxSZXN1bHRzZXROb3RGb3VuZCII...' → 'SqlResultsetNotFound'
        → 'QSI-0006: Unable to resolve table ...'
    실패하면 원본을 그대로 돌려준다.
    """
    if not message:
        return ""
    try:
        raw = base64.b64decode(message + "=" * (-len(message) % 4))
    except Exception:
        return message
    texts = []

    def walk(items):
        for _, kind, val in items:
            if kind == "str" and val.strip():
                texts.append(val.strip())
            elif kind == "msg":
                walk(val)

    walk(decode_raw(raw))
    # 중첩 메시지가 문자열로 잡히면 맨 앞에 protobuf 길이 바이트가 남는다.
    # 그 바이트 값이 뒤에 남은 길이와 같을 때만 떼어낸다 (본문 첫 글자를 지우지
    # 않도록). 가장 긴 것이 본문이고 나머지는 'ENGINE' 같은 발생 지점 태그다.
    cleaned = []
    for t in texts:
        while len(t) >= 2 and ord(t[0]) == len(t) - 1:
            t = t[1:]
        m = re.search(r"[A-Za-z가-힣][\x20-\x7e가-힣]*", t)
        if m:
            cleaned.append(m.group().strip())
    return max(cleaned, key=len) if cleaned else message


def _is_auth_error(status, message):
    """access token 만료류 인증 오류인지 판정 (refresh 트리거용). SessionNotFound 등은 제외."""
    if status not in ("10", "16") or not message:
        return False
    try:
        mb = base64.b64decode(message + "=" * (-len(message) % 4))
    except Exception:
        mb = message.encode("utf-8", "replace")
    return b"token" in mb.lower()


def _refresh_tokens(insecure=False, window_id=""):
    """RefreshToken(빈 body, 쿠키 인증) → 응답 #1(access)/#3(refresh)로 쿠키 파일 갱신.
    성공 시 새 access token 반환, 실패(refresh token 만료 등) 시 None."""
    try:
        frames = call("api.user.AccountService", "RefreshToken", b"",
                      insecure, window_id=window_id, _allow_refresh=False)
    except SystemExit:
        return None
    if not frames:
        return None
    access = extract_field_raw(frames[0], 1)
    refresh = extract_field_raw(frames[0], 3)
    if not access:
        return None
    _update_cookie_file(
        access=access.decode("utf-8", "replace"),
        refresh=refresh.decode("utf-8", "replace") if refresh else None,
    )
    return access.decode("utf-8", "replace")


# 메모리로 공급받은 credential 과, 파일을 쓸지 여부.
# 프록시는 클라이언트가 접속할 때 받은 계정을 메모리에만 두고 파일을 건드리지 않는다
# (LOGIN_FILE 은 CLI 전용으로 남는다). CLI 는 이 값을 건드리지 않아 기존대로 파일을 쓴다.
_MEM_CRED = None
_USE_LOGIN_FILE = True


def set_credential(username=None, password=None, use_file=True):
    """메모리 credential 을 설정한다. use_file=False 면 LOGIN_FILE 을 읽지 않는다."""
    global _MEM_CRED, _USE_LOGIN_FILE
    _MEM_CRED = (username, password) if username and password else None
    _USE_LOGIN_FILE = use_file


def load_login():
    """로그인 credential (username, password). 없거나 불완전하면 None.

    메모리로 받은 값이 있으면 그것을 먼저 쓴다. 파일은 사용자가 직접 만든다:
    {"username": "...", "password": "..."}
    값은 어떤 경로로도 stdout 에 출력하지 않는다.
    """
    if _MEM_CRED:
        return _MEM_CRED
    if not _USE_LOGIN_FILE:
        return None
    if not os.path.exists(LOGIN_FILE):
        return None
    try:
        with open(LOGIN_FILE, encoding="utf-8") as fp:
            d = json.load(fp)
    except (OSError, ValueError):
        info(f"[login] {os.path.basename(LOGIN_FILE)} 를 읽을 수 없습니다 (JSON 형식 확인).")
        return None
    user, pw = d.get("username"), d.get("password")
    return (user, pw) if user and pw else None


def login(insecure=False, window_id="", cred=None):
    """`api.user.AccountService/authenticate` 로 로그인해 쿠키 파일을 갱신 (성공 시 True).

    요청은 문자열 2필드(#1 username / #2 password)뿐이고, 서버가 응답 `Set-Cookie` 로
    qp_access_token(Path=/) 과 qp_refresh_token(Path 는 RefreshToken 경로 한정) 을 심는다.
    브라우저와 달리 이 CLI 는 쿠키를 모든 요청에 직접 붙이므로 Path 제약은 문제되지 않는다.
    쿠키 파일이 없어도 동작해야 하므로 call() 대신 직접 요청한다.
    """
    cred = cred or load_login()
    if not cred:
        return False
    headers = {
        "content-type": "application/grpc-web-text",
        "accept": "application/grpc-web-text",
        "x-grpc-web": "1",
        "x-user-agent": "grpc-web-javascript/0.1",
    }
    if window_id:
        headers["x-querypie-window-id"] = window_id
    try:
        status_code, reason, hdr_list, _ = http_post(
            "/engine-grpc/api.user.AccountService/authenticate",
            base64.b64encode(frame(f_str(1, cred[0]) + f_str(2, cred[1]))),
            headers, insecure, 60)
    except (http.client.HTTPException, OSError) as e:
        info(f"[login] 실패 — 연결 오류: {e} (VPN 연결 상태를 확인하세요)")
        return False
    if status_code >= 400:
        info(f"[login] 실패 — HTTP {status_code} {reason}")
        return False
    set_cookies = [v for k, v in hdr_list if k.lower() == "set-cookie"]
    lower = {k.lower(): v for k, v in hdr_list}
    status = lower.get("grpc-status")
    message = lower.get("grpc-message")

    tokens = {}
    for sc in set_cookies:
        name, _, rest = sc.partition("=")
        name = name.strip()
        if name in ("qp_access_token", "qp_refresh_token"):
            tokens[name] = rest.split(";", 1)[0].strip()
    if "qp_access_token" not in tokens:
        detail = f" ({humanize_grpc(message)})" if status not in (None, "0") else ""
        info(f"[login] 실패 — 응답에 인증 쿠키가 없습니다{detail}. "
             f"{os.path.basename(LOGIN_FILE)} 의 username/password 를 확인하세요.")
        return False
    _update_cookie_file(access=tokens["qp_access_token"], refresh=tokens.get("qp_refresh_token"))
    info("[login] credential 로 로그인해 쿠키를 갱신했습니다"
         f"{'' if 'qp_refresh_token' in tokens else ' (refresh 쿠키는 응답에 없음)'}.")
    return True


# --------------------------------------------------------------------------
# SQLService / ConnectionService
# --------------------------------------------------------------------------
def sql_parse(conn, db, sql, oid, db_type_enum=1, insecure=False, window_id=""):
    """SQLParseRequest: #2 conn #3 databaseType #4 db #5 sqlText #6 oid
    응답 #3.#1 에 서버가 발급한 sqlId 가 담긴다 (execute/getDataTable 이 공유)."""
    payload = (f_str(2, conn) + f_vi(3, db_type_enum) + f_str(4, db)
               + f_str(5, sql) + f_str(6, oid))
    return call(SVC_SQL, "parse", payload, insecure, window_id=window_id)


def sql_execute(conn, db, sql, parser_result, use_limit=1, limit_count=1000,
                insecure=False, window_id=""):
    """SQLExecutionRequest
    #2 connectionUuid #3 databaseName #4 sqlText
    #5 = parse 응답의 #3(ParserResult) 원본 그대로 (sqlId/구문분석 결과 포함)
    #6 useLimit #7 limitCount
    """
    payload = (f_str(2, conn) + f_str(3, db) + f_str(4, sql)
               + f_msg(5, parser_result) + f_vi(6, use_limit) + f_vi(7, limit_count))
    return call(SVC_SQL, "execute", payload, insecure, window_id=window_id)


def sql_get_data_table(conn, db, sql_id, start_row=0, row_count=200,
                       session_id="", insecure=False, window_id=""):
    """DataTableRequest
    #1 sessionId #2 connectionUuid #3 databaseName #4 sqlId #5 startRow #6 rowCount
    """
    payload = (f_str(1, session_id) + f_str(2, conn) + f_str(3, db)
               + f_str(4, sql_id) + f_vi(5, start_row) + f_vi(6, row_count))
    return call(SVC_SQL, "getDataTable", payload, insecure, window_id=window_id)


def sql_clear_execute(conn, db, sql_id, session_id="", insecure=False, window_id=""):
    """SQLExecutionClearRequest: #1 sessionId #2 connectionUuid #3 databaseName #4 sqlId"""
    payload = (f_str(1, session_id) + f_str(2, conn) + f_str(3, db) + f_str(4, sql_id))
    return call(SVC_SQL, "clearExecute", payload, insecure, window_id=window_id)


def get_connection(conn_uuid, kind=3, insecure=False, window_id=""):
    return call(SVC_CONN, "getConnection", f_vi(1, kind) + f_str(2, conn_uuid),
                insecure, window_id=window_id)


def session_open(open_payload, insecure=False, window_id=""):
    """engine.session.SessionService/open — connectionUuid 에 대한 SQL 세션을 서버에 확립.

    parse/execute/getDataTable 이 connectionUuid 로 이 세션을 참조하므로 반드시 선행돼야 한다.
    open_payload 는 resolve_open_payload 가 조립한 connection 객체를 담는다.
    """
    return call("engine.session.SessionService", "open", open_payload,
                insecure, window_id=window_id)


# --------------------------------------------------------------------------
# LOB — 긴 값은 내용 대신 핸들로 온다
# --------------------------------------------------------------------------
# 서버는 긴 셀을 {"id":"<uuid>","preview":"앞부분...","type":"CLOB"} 로 내려준다.
# 그대로 두면 EXPLAIN FORMAT=JSON 이나 긴 TEXT/JSON 컬럼이 미리보기에서 잘린 채 보이므로
# largeObjectView 로 전체 값을 받아 채운다. 셀 하나마다 왕복이 한 번 더 든다.
LOB_TYPES = {"CLOB", "BLOB", "JSON", "XML", "TEXT"}
LOB_MAX_BYTES = 1 << 20
LOB_CELLS = 200


def lob_handle(value):
    """셀 값이 LOB 핸들이면 파싱해서 반환, 아니면 None."""
    if not isinstance(value, str) or not value.startswith('{"id":'):
        return None
    try:
        h = json.loads(value)
    except ValueError:
        return None
    if isinstance(h, dict) and h.get("type") in LOB_TYPES and "id" in h and "preview" in h:
        return h
    return None


def fetch_lob(value_id, conn, db, max_bytes=LOB_MAX_BYTES, insecure=False, window_id=""):
    """largeObjectView 로 LOB 전체 내용을 받는다 (없으면 None).

    contentRange(#5 = {시작, 끝})는 필수다. 빠뜨리면 서버가 NullReferenceException 을 낸다.
    """
    payload = (f_str(2, conn) + f_str(3, db or "") + f_str(4, value_id)
               + f_msg(5, f_vi(1, 0) + f_vi(2, max_bytes)))
    for d in call(SVC_SQL, "largeObjectView", payload, insecure, window_id=window_id):
        content = extract_field_raw(d, 7)
        if content is not None:
            return content
    return None


def expand_lobs(rows, conn, db, max_cells=LOB_CELLS, max_bytes=LOB_MAX_BYTES,
                insecure=False, window_id="", notify=None):
    """행 안의 LOB 핸들을 실제 값으로 바꾼다. 같은 값은 한 번만 받아 재사용한다.

    notify 는 한 줄 메시지를 받는 콜백이다 (기본은 info). 조회가 실패한 셀은 미리보기
    문자열로 남겨 둔다 — 값 하나 때문에 결과 전체를 잃지 않게 한다.
    """
    notify = notify or info
    if not max_cells:
        return rows
    cache, fetched, skipped = {}, 0, 0
    for row in rows:
        for i, cell in enumerate(row):
            handle = lob_handle(cell)
            if not handle:
                continue
            vid = handle["id"]
            if vid not in cache:
                if fetched >= max_cells:
                    skipped += 1
                    continue
                fetched += 1
                try:
                    raw = fetch_lob(vid, conn, db, max_bytes, insecure, window_id)
                except SystemExit as e:
                    notify(f"[lob] 조회 실패, 미리보기로 대체: {str(e)[:120]}")
                    raw = None
                # BLOB 은 바이너리 그대로, 나머지는 텍스트로 넘긴다
                cache[vid] = (raw if handle.get("type") == "BLOB"
                              else raw.decode("utf-8", "replace")) if raw is not None else None
            if cache[vid] is not None:
                row[i] = cache[vid]
    if skipped:
        notify(f"[lob] {skipped}개는 상한({max_cells}셀)을 넘어 미리보기로 남겼습니다")
    return rows


def main():
    ap = argparse.ArgumentParser(description="QueryPie gRPC-Web 조회 클라이언트")
    ap.add_argument("--conn", help="connectionUuid")
    ap.add_argument("--conn-name", help="세션 페이로드 이름 (querypie-open-<name>.b64). "
                                        "미지정 시 --db 로 querypie-conn-map.json / 같은 이름 파일에서 자동 결정")
    ap.add_argument("--db", help="databaseName")
    ap.add_argument("--sql", help="실행할 SQL")
    ap.add_argument("--sql-file", help="SQL 파일 경로")
    ap.add_argument("--session", default="", help="sessionId")
    ap.add_argument("--sql-id", default="", help="sqlId (미지정 시 session 또는 HAR 값)")
    ap.add_argument("--rows", type=int, default=200,
                    help="가져올 행 수 (서버측 상한도 이 값으로 맞춘다). 결과가 이 수를 "
                         "정확히 채우면 뒤가 잘렸을 수 있다는 경고가 나온다")
    ap.add_argument("--start-row", type=int, default=0,
                    help="이 행부터 가져온다 (잘린 결과를 이어 받을 때)")
    ap.add_argument("--db-type", default="MySql")
    ap.add_argument("--use-limit", type=int, default=0)
    ap.add_argument("--limit-count", type=int, default=0,
                    help="서버가 결과셋을 만들 때 자르는 행 수. 기본은 --rows 와 같으므로 "
                         "따로 줄 일이 거의 없다 (--rows 보다 작게 주면 그 값이 상한이 된다)")
    ap.add_argument("--clear-first", action="store_true", help="execute 전에 clearExecute 호출")
    ap.add_argument("--get-connection", action="store_true", help="커넥션 메타만 조회")
    ap.add_argument("--dump", action="store_true", help="execute 응답 raw 구조도 출력")
    ap.add_argument("--insecure", action="store_true", help="TLS 검증 생략")
    ap.add_argument("--tsv", action="store_true",
                    help="값을 자르지 않고 탭 구분으로 출력 (백업용. 진행 메시지는 stderr 로 나가므로 "
                         "'> out.tsv' 로 데이터만 파일에 담긴다)")
    ap.add_argument("--max-col-width", type=int, default=80,
                    help="표 모드 컬럼폭 상한 (기본 80). 넘는 값은 말줄임되므로 긴 값은 --tsv 를 쓸 것")
    ap.add_argument("--lob-cells", type=int, default=LOB_CELLS,
                    help=f"긴 값(LOB)을 전체 내용으로 복원할 셀 수 상한 (기본 {LOB_CELLS}, 0 이면 "
                         f"복원하지 않고 서버가 준 미리보기 핸들 그대로 둔다). 셀마다 왕복이 한 번 더 든다")
    ap.add_argument("--lob-max-bytes", type=int, default=LOB_MAX_BYTES,
                    help=f"LOB 한 개당 받아올 최대 바이트 (기본 {LOB_MAX_BYTES})")
    ap.add_argument("--writer", action="store_true",
                    help="읽기 엔드포인트 대신 쓰기 엔드포인트로 붙는다 (복제 지연 없이 "
                         "방금 바뀐 값을 봐야 할 때). 읽기 엔드포인트가 없는 클러스터는 무관하다")
    ap.add_argument("--window-id", default="", help="x-querypie-window-id (미지정 시 랜덤 32-hex 생성)")
    ap.add_argument("--no-refresh", action="store_true", help="조회 전 선제 토큰 갱신 생략")
    ap.add_argument("--login", action="store_true",
                    help=f"조회 전 credential({os.path.basename(LOGIN_FILE)})로 강제 재로그인")
    args = ap.parse_args()

    global _TSV
    _TSV = args.tsv

    # window-id 는 UI 창 식별자다. 서버는 빈 값만 거부하므로 미지정 시 랜덤 생성해
    # execute/getDataTable/clearExecute 가 같은 값을 공유하게 한다.
    window_id = args.window_id or secrets.token_hex(16)

    # 선제 refresh: 조회 전 access token 을 미리 갱신해 22분 만료를 회피한다.
    # QueryPie 는 access token 이 유효할 때만 RefreshToken 을 허용하는 슬라이딩 세션이므로
    # (만료 후엔 재로그인 필요) 만료를 기다리지 않고 매 실행 시작 시 갱신한다.
    # rotation 이라 refresh token 도 함께 갱신된다.
    # 실패해도 저장된 access token 이 살아 있으면 조회는 되므로 중단하지는 않되,
    # 조용히 넘기면 20분 뒤 엉뚱한 Invalid token 으로 드러나므로 사유를 밝힌다.
    # 강제 재로그인 / 쿠키 파일이 아예 없을 때의 최초 획득
    if args.login and not login(args.insecure, window_id):
        sys.exit(f"--login 실패. {LOGIN_FILE} 를 확인하세요.")
    if not has_cookies() and not login(args.insecure, window_id):
        sys.exit(f"쿠키가 없고 자동 로그인도 하지 못했습니다.\n"
                 f'  {LOGIN_FILE} 에 {{"username": ..., "password": ...}} 를 두거나,\n'
                 f'  {COOKIE_FILE} 에 {{"cookies": {{"qp_access_token": ...}}}} 를 저장하세요.')

    if not args.no_refresh:
        if _refresh_tokens(args.insecure, window_id):
            info("[refresh] 선제 토큰 갱신 완료")
        elif login(args.insecure, window_id):
            pass  # refresh 가 죽었으면 credential 로 재로그인 (메시지는 login 이 남긴다)
        elif "qp_refresh_token" not in load_cookies():
            info("[refresh] 갱신 생략 — 쿠키 파일에 qp_refresh_token 이 없습니다. "
                 "저장된 access token 의 남은 수명(발급 후 20분)만 쓸 수 있습니다.")
        else:
            info("[refresh] 갱신 실패 — refresh token 이 무효/만료입니다(브라우저가 먼저 rotation 하면 "
                 f"파일의 값은 폐기됩니다). {os.path.basename(LOGIN_FILE)} 에 username/password 를 "
                 "두면 자동 로그인으로 해결됩니다. 수동 복사는 DevTools Network 탭의 "
                 "AccountService/RefreshToken 요청 Request Headers > Cookie 에서 access 와 refresh 를 "
                 "함께 가져오세요.")

    conn, db = args.conn, args.db
    db = db or config().get("default_database") or ""
    if not db and not args.conn_name and not conn:
        ap.error("--db 가 필요합니다 (querypie-config.json 의 default_database 로 기본값을 둘 수 있습니다)")
    conn_name = resolve_conn_name(db, args.conn_name)
    open_payload, src = resolve_open_payload(conn_name, args.insecure, window_id,
                                             prefer_writer=args.writer)
    if open_payload:
        info(f"[open] {src}")
    else:
        ap.error(f"'{db}' 에 쓸 커넥션을 찾지 못했습니다. --conn-name 으로 커넥션을 지정하거나 "
                 f"querypie-conn-map.json 에 database 매핑을 추가하세요 "
                 f"(후보는 querypie_conn.py list 로 확인합니다)")
    # open 페이로드에서 connectionUuid(#2) 자동 추출
    if not conn and open_payload:
        c = extract_field_raw(open_payload, 2)
        if c:
            conn = c.decode("utf-8", "replace")
    conn = conn or load_connection()

    if args.get_connection:
        if not conn:
            ap.error("--get-connection 에는 --conn / 세션 페이로드 / querypie-connection.txt 중 하나가 필요합니다")
        for d in get_connection(conn, insecure=args.insecure):
            show(decode_raw(d))
        return

    sql = args.sql
    if args.sql_file:
        with open(args.sql_file, encoding="utf-8") as fp:
            sql = fp.read()
    if not conn:
        ap.error("connectionUuid 가 필요합니다: --conn / 세션 페이로드 / secrets\\querypie-connection.txt 중 하나")
    if not sql:
        ap.error("--sql 또는 --sql-file 이 필요합니다")

    # 0) SessionService/open 으로 connectionUuid 에 SQL 세션을 확립한다.
    #    (parse/execute/getDataTable 이 connectionUuid 로 이 세션을 참조)
    info("== session open ==")
    session_open(open_payload, args.insecure, window_id)

    # 1) parse 로 sqlId 발급 (execute/getDataTable 이 공유)
    _h = secrets.token_hex(16)
    oid = f"{_h[:8]}-{_h[8:12]}-{_h[12:16]}-{_h[16:20]}-{_h[20:]}"
    info("== parse ==")
    sql_id = args.sql_id
    parser_result = None
    for d in sql_parse(conn, db, sql, oid,
                       {"MySql": 1}.get(args.db_type, 1), args.insecure, window_id):
        # parse 응답 #3 = ParserResult. execute 의 #5 에 그대로 전달한다.
        pr = extract_field_raw(d, 3)
        if pr:
            parser_result = pr
            # sqlId 는 Kestrel trace id 형식(0H...). ParserResult 안에 박혀 있다.
            m = re.search(rb"0H[0-9A-Z]{8,}", pr)
            if m:
                sql_id = m.group().decode()
    if not (parser_result and sql_id):
        sys.exit("parse 응답에서 ParserResult / sqlId 를 찾지 못했습니다.")
    info(f"   sqlId 발급 완료 (len={len(sql_id)})")

    if args.clear_first:
        info("== clearExecute ==")
        sql_clear_execute(conn, db, sql_id, args.session, args.insecure, window_id)

    # execute 의 limitCount 는 서버가 결과셋을 만들 때 자르는 값이라, getDataTable 에 아무리
    # 큰 --rows 를 줘도 이 값을 넘지 못한다. 둘을 따로 두면 --rows 만 올린 조회가 조용히
    # 잘리므로 기본값을 --rows 에 맞춘다 (--limit-count 를 직접 준 경우에는 그 값을 쓴다).
    limit_count = args.limit_count or args.rows
    info("== execute ==")
    for d in sql_execute(conn, db, sql, parser_result,
                         args.use_limit or 1, limit_count,
                         args.insecure, window_id):
        if args.dump:
            show(decode_raw(d))

    info("== getDataTable ==")
    frames = sql_get_data_table(conn, db, sql_id, args.start_row, args.rows,
                                args.session, args.insecure, window_id)
    shown = render_data_table(frames, args.max_col_width, args.tsv,
                              expand=lambda rows: expand_lobs(rows, conn, db, args.lob_cells,
                                                              args.lob_max_bytes, args.insecure,
                                                              window_id))
    # 상한을 정확히 채웠으면 그 뒤가 더 있는지 알 수 없다 — 서버는 잘렸다고 알려주지 않는다.
    cap = min(args.rows, limit_count)
    if shown and shown >= cap:
        info(f"[주의] 상한 {cap}행을 정확히 채웠습니다 — 뒤가 잘렸을 수 있습니다. "
             f"--rows 를 늘려 다시 조회하거나 --start-row 로 이어 받으세요.")
    if args.dump:
        for d in frames:
            show(decode_raw(d))


if __name__ == "__main__":
    main()
