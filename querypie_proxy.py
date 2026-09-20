#!/usr/bin/env python3
r"""QueryPie 를 백엔드로 쓰는 로컬 MySQL 프록시.

HeidiSQL 같은 평범한 MySQL 클라이언트가 localhost 로 붙으면, 받은 쿼리를 QueryPie
gRPC-Web 경로(querypie_query.py)로 중계하고 결과를 MySQL 프로토콜로 돌려준다.
쿼리는 그대로 QueryPie 를 거치므로 접근 권한과 감사 로그는 QueryPie 정책을 따른다.

실행:
  python querypie_proxy.py                      # 기본 커넥션, 127.0.0.1:3307
  python querypie_proxy.py --conn-name archive --port 3308
  python querypie_proxy.py --allow-write        # 쓰기 문장까지 통과 (기본은 읽기 전용)

HeidiSQL 접속 설정:
  네트워크 유형 = MariaDB or MySQL (TCP/IP)
  호스트 = 127.0.0.1 / 포트 = 3307
  사용자, 암호 = 기본은 아무 값이나 통과한다 (QueryPie 계정은 querypie-login.json 을 쓴다).
    --login-from-client 를 켜면 여기에 QueryPie 계정을 넣어야 하고, 그 값으로 로그인한다.

동작 요약:
  - 커넥션은 이름(클러스터 이름의 앞부분)만 주면 그 클러스터의 엔드포인트로 그 자리에서
    조립한다. 노드를 박제해 두지 않으므로 노드가 교체돼도 그대로 붙는다. 읽기 전용일 때는
    읽기 엔드포인트로, `--allow-write` 일 때는 쓰기 엔드포인트로 붙는다.
  - 오래 쉬면 서버가 세션을 정리한다. 그때 오는 `SessionNotFound` 는 클라이언트에 보이지
    않게 세션을 다시 열고 보고 있던 database 까지 되돌린 뒤 한 번 더 시도한다.
  - QueryPie 세션은 프로세스당 하나를 열어 재사용한다 (열기 약 0.2초).
  - `USE db` / 접속 시 지정한 DB 는 TransactionService/changeDatabase 로 반영하므로
    `db.table` 한정 없이 쓸 수 있다. 클라이언트 커넥션마다 현재 DB 를 따로 기억한다.
  - `SET ...` 처럼 결과셋이 없는 문장은 QueryPie 가 SqlResultsetNotFound 로 거부하므로
    서버로 보내지 않고 프록시가 OK 로 응답한다 (클라이언트 접속 절차용).
  - 일반 쿼리는 왕복 3회(parse/execute/getDataTable)라 약 0.3초가 걸린다.
  - 개체 탐색(테이블 목록, DDL, 트리거·함수·외래키 유무 확인 등)은 QueryPie UI 가 쓰는
    dictionary API 로 대신 처리해 20~40ms 에 끝낸다. 시작 시 컬럼 헤더를 예열해 두므로
    첫 접속부터 적용된다 (--no-dictionary / --no-warmup 으로 끌 수 있다).

한계:
  - 값은 언제나 텍스트 프로토콜로 내려간다. 컬럼 타입은 QueryPie 가 알려주는 CLR 타입을
    MySQL 타입으로 옮겨 붙이므로(CLR_TO_MYSQL) 정렬과 표시는 타입대로 동작한다.
  - prepared statement(COM_STMT_*)는 지원하지 않는다. 텍스트 프로토콜만 쓴다.
  - 트랜잭션 제어문은 프록시가 OK 로 삼킨다 (QueryPie 세션이 문장 단위로 동작).
  - 인증을 검사하지 않으므로 127.0.0.1 에만 바인딩한다.
"""
import argparse
import concurrent.futures as cf
import os
import re
import secrets
import socketserver
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 선택 의존성(sqlglot)은 도구 전용 vendor 에 둔다. 시스템 파이썬은 PEP 668 로 잠겨 있고,
# 여기에 두면 사용자 환경을 건드리지 않으면서 프록시가 자체적으로 로드할 수 있다.
#   설치: python3 -m pip install --target <이 파일 옆>/vendor sqlglot
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))

import querypie_query as qp  # noqa: E402  (경로 삽입 후 import)

try:
    import sqlglot
    from sqlglot import exp as sqlglot_exp
except ImportError:
    sqlglot = None
    sqlglot_exp = None

SVC_TX = "engine.transaction.TransactionService"
SVC_SQL = "engine.sql.SQLService"
# QueryPie UI 의 개체 탐색용 API. 같은 정보를 SQL 로 묻는 것보다 10배 이상 빠르다
# (getTables 19ms vs SHOW TABLE STATUS 273ms, getTableScript 26ms vs SHOW CREATE TABLE
# 273ms + LOB 왕복). parse/execute/getDataTable 3왕복을 1왕복으로 줄이기 때문이다.
# 요청은 공통으로 {#2 connectionUuid, #3 database, (#4 table)} 이다.
SVC_DICT_TABLE = "engine.dictionary.table.TableDictionaryService"
SVC_DICT_DB = "engine.dictionary.database.DatabaseDictionaryService"
SVC_DICT_TRIGGER = "engine.dictionary.trigger.TriggerDictionaryService"
SVC_DICT_FUNC = "engine.dictionary.func.FunctionDictionaryService"
SVC_DICT_PROC = "engine.dictionary.procedure.ProcedureDictionaryService"
SVC_DICT_EVENT = "engine.dictionary.event.EventDictionaryService"
SVC_DICT_VIEW = "engine.dictionary.view.ViewDictionaryService"

# 시작 로그에 찍어 실행 중인 프로세스가 최신 파일을 쓰는지 확인하는 용도.
# 동작을 바꿀 때마다 올린다 (프록시를 재시작하지 않으면 수정이 반영되지 않는다).
VERSION = "1.8 (로그에 시각 표기)"

# 긴 값(LOB)은 QueryPie 가 내용 대신 핸들만 내려준다. 이대로 두면 SHOW CREATE TABLE 이
# 잘린 문자열이 되어 클라이언트가 테이블 구조를 파싱하지 못한다(기본키 인식 실패의 원인).
# 복원은 querypie_query 의 expand_lobs 가 맡는다.

# DDL 파싱 결과를 붙들어 두는 시간. 테이블 하나를 여는 동안의 연속 조회만
# 묶으려는 것이라 짧게 둔다 (스키마 변경이 곧바로 반영되어야 한다).
DDL_CACHE_TTL = 1.0

# 서버가 광고하는 capability. CLIENT_DEPRECATE_EOF 를 빼서 결과셋 종료를 항상
# EOF 패킷으로 통일한다(클라이언트도 협상 결과에 따라 EOF 를 기대하게 된다).
CAP_LONG_PASSWORD = 0x00000001
CAP_FOUND_ROWS = 0x00000002
CAP_LONG_FLAG = 0x00000004
CAP_CONNECT_WITH_DB = 0x00000008
CAP_PROTOCOL_41 = 0x00000200
CAP_TRANSACTIONS = 0x00002000
CAP_SECURE_CONNECTION = 0x00008000
CAP_PLUGIN_AUTH = 0x00080000
CAP_CONNECT_ATTRS = 0x00100000
SERVER_CAPS = (CAP_LONG_PASSWORD | CAP_FOUND_ROWS | CAP_LONG_FLAG | CAP_CONNECT_WITH_DB
               | CAP_PROTOCOL_41 | CAP_TRANSACTIONS | CAP_SECURE_CONNECTION
               | CAP_PLUGIN_AUTH | CAP_CONNECT_ATTRS)

STATUS_AUTOCOMMIT = 0x0002
CHARSET_UTF8MB4 = 45
CHARSET_BINARY = 63
TYPE_VAR_STRING = 0xFD

# 컬럼 플래그 (클라이언트가 PK 를 인식해야 행 복제, 인라인 편집이 제대로 동작한다)
FLAG_NOT_NULL = 0x0001
FLAG_PRI_KEY = 0x0002
FLAG_BINARY = 0x0080
FLAG_NUM = 0x8000

# QueryPie 가 알려주는 CLR 타입 → MySQL 컬럼 타입.
# 값 자체는 텍스트 프로토콜로 나가지만, 타입을 맞춰야 클라이언트가 정렬과 표시를
# 문자열이 아닌 실제 타입 기준으로 처리한다.
CLR_TO_MYSQL = {
    "System.Boolean": (0x01, FLAG_NUM),          # TINY
    "System.SByte": (0x01, FLAG_NUM),
    "System.Byte": (0x01, FLAG_NUM),
    "System.Int16": (0x02, FLAG_NUM),            # SHORT
    "System.UInt16": (0x02, FLAG_NUM),
    "System.Int32": (0x03, FLAG_NUM),            # LONG
    "System.UInt32": (0x03, FLAG_NUM),
    "System.Int64": (0x08, FLAG_NUM),            # LONGLONG
    "System.UInt64": (0x08, FLAG_NUM),
    "System.Single": (0x04, FLAG_NUM),           # FLOAT
    "System.Double": (0x05, FLAG_NUM),           # DOUBLE
    "System.Decimal": (0xF6, FLAG_NUM),          # NEWDECIMAL
    "System.DateTime": (0x0C, 0),                # DATETIME
    "MySqlConnector.MySqlDateTime": (0x0C, 0),
    "System.DateOnly": (0x0A, 0),                # DATE
    "System.TimeSpan": (0x0B, 0),                # TIME
    "MySqlConnector.MySqlTimeSpan": (0x0B, 0),
    "System.Byte[]": (0xFC, FLAG_BINARY),        # BLOB
    "System.Guid": (TYPE_VAR_STRING, 0),
    "System.String": (TYPE_VAR_STRING, 0),
}
ERR_UNKNOWN = 1105  # ER_UNKNOWN_ERROR — QueryPie 오류를 그대로 전달할 때 쓴다

COM_QUIT, COM_INIT_DB, COM_QUERY, COM_FIELD_LIST = 0x01, 0x02, 0x03, 0x04
COM_STATISTICS, COM_PING = 0x09, 0x0E

# 미지원 명령을 로그에 남길 때 코드 대신 이름을 보여준다
COMMAND_NAMES = {
    0x00: "COM_SLEEP", 0x05: "COM_CREATE_DB", 0x06: "COM_DROP_DB", 0x07: "COM_REFRESH",
    0x08: "COM_SHUTDOWN", 0x0A: "COM_PROCESS_INFO", 0x0C: "COM_PROCESS_KILL",
    0x0D: "COM_DEBUG", 0x0F: "COM_TIME", 0x11: "COM_CHANGE_USER",
    0x16: "COM_STMT_PREPARE", 0x17: "COM_STMT_EXECUTE", 0x19: "COM_STMT_CLOSE",
    0x1A: "COM_STMT_RESET", 0x1B: "COM_SET_OPTION", 0x1C: "COM_STMT_FETCH",
    0x1F: "COM_RESET_CONNECTION",
}

# 결과셋 없이 OK 로 삼킬 문장 (클라이언트 접속, 세션 설정용)
RE_SWALLOW = re.compile(
    r"^\s*(set|start\s+transaction|begin|commit|rollback|unlock\s+tables|"
    r"lock\s+tables|flush\b|do\b)\b", re.I)
RE_USE = re.compile(r"^\s*use\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?\s*;?\s*$", re.I)
# 읽기 전용 모드에서 허용할 문장
RE_READONLY = re.compile(r"^\s*(select|show|describe|desc|explain|with|analyze)\b", re.I)
# 기본키 보완은 단일 테이블 SELECT 에만 적용한다 (SHOW 문에 적용하면 재귀가 된다)
RE_SELECT = re.compile(r"^\s*select\b", re.I)
# dictionary API 로 바로 답할 수 있는 문장. 결과 컬럼이 SQL 응답과 같은 것만 다룬다.
RE_SHOW_TABLES = re.compile(r"^\s*show\s+tables(?:\s+from\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?)?\s*$", re.I)
RE_SHOW_CREATE = re.compile(r"^\s*show\s+create\s+table\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?"
                            r"(?:\s*\.\s*[`\"]?([A-Za-z0-9_$]+)[`\"]?)?\s*$", re.I)
RE_SHOW_DATABASES = re.compile(r"^\s*show\s+(databases|schemas)\s*$", re.I)
RE_SCHEMA_COLLATION = re.compile(
    r"^\s*select\s+`?default_collation_name`?\s+from\s+`?information_schema`?\s*\.\s*"
    r"`?schemata`?\s+where\s+`?schema_name`?\s*=\s*'([A-Za-z0-9_$]+)'\s*$", re.I | re.S)
# CHECK 제약 조회. 실제로 걸린 테이블은 거의 없어 대개 0행이다.
RE_CHECK_CONSTRAINTS = re.compile(
    r"^\s*select\b.*?\bfrom\s+`?information_schema`?\s*\.\s*`?check_constraints`?\b"
    r".*?constraint_schema\s*=\s*'([A-Za-z0-9_$]+)'"
    r".*?table_name\s*=\s*'([A-Za-z0-9_$]+)'", re.I | re.S)
RE_SHOW_TABLE_STATUS = re.compile(
    r"^\s*show\s+table\s+status(?:\s+from\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?)?"
    r"(?:\s+like\s+'([^']*)')?\s*$", re.I)
# SHOW KEYS FROM `t` [FROM `db`] / SHOW KEYS FROM `db`.`t` (실험 기능에서만 쓴다)
RE_SHOW_KEYS = re.compile(
    r"^\s*show\s+(?:index|indexes|keys)\s+from\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?"
    r"(?:\s*\.\s*[`\"]?([A-Za-z0-9_$]+)[`\"]?)?"
    r"(?:\s+from\s+[`\"]?([A-Za-z0-9_$]+)[`\"]?)?\s*$", re.I)

# SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='x' AND TABLE_NAME='y'
RE_IS_COLUMNS = re.compile(
    r"^\s*select\s+\*\s+from\s+`?information_schema`?\s*\.\s*`?columns`?\b"
    r".*?table_schema\s*=\s*'([A-Za-z0-9_$]+)'"
    r".*?table_name\s*=\s*'([A-Za-z0-9_$]+)'", re.I | re.S)

# GUI 는 DB, 테이블을 열 때마다 "없는 것"을 확인하려고 무거운 조회를 줄줄이 던진다.
# (트리거, 함수, 프로시저, 이벤트, 외래키 ...) 실제 운영 스키마에서는 대부분 0행인데
# 한 건에 0.28초씩 들어 DB 하나 여는 데만 2초가 이 확인에 쓰인다.
# dictionary API 로 "있는지" 만 20~100ms 에 확인하고, 없으면 빈 결과로 답한다.
#
# 컬럼 헤더는 지어내지 않는다. 그 패턴을 처음 만났을 때는 SQL 로 실행해 서버가 준
# 헤더를 배워 두고(그 뒤로 재사용), 배우기 전에는 그냥 SQL 로 넘긴다. 이렇게 하면
# 헤더는 언제나 실제 서버 것과 같고, 행이 있는 경우에도 항상 SQL 결과가 나간다.
#   (키, 정규식, dictionary 서비스, 메서드, 테이블 인자를 쓰는가)
EMPTY_CHECK_RULES = [
    ("triggers", re.compile(r"^\s*show\s+triggers\s+from\s+[`\"]?([A-Za-z0-9_$]+)", re.I),
     SVC_DICT_TRIGGER, "getTriggers", False),
    ("functions", re.compile(r"^\s*show\s+function\s+status\b.*?['`\"]([A-Za-z0-9_$]+)['`\"]", re.I | re.S),
     SVC_DICT_FUNC, "getFunctions", False),
    ("procedures", re.compile(r"^\s*show\s+procedure\s+status\b.*?['`\"]([A-Za-z0-9_$]+)['`\"]", re.I | re.S),
     SVC_DICT_PROC, "getProcedures", False),
    ("events", re.compile(r"^\s*select\b.*?\bfrom\s+`?information_schema`?\s*\.\s*`?events`?\b"
                          r".*?event_schema\s*=\s*'([A-Za-z0-9_$]+)'", re.I | re.S),
     SVC_DICT_EVENT, "getEvents", False),
    # 외래키 두 종류. 조건에 REFERENCED_TABLE_NAME IS NOT NULL 이 있어야 외래키 조회다.
    ("fk_ref", re.compile(r"^\s*select\b.*?\bfrom\s+`?information_schema`?\s*\.\s*`?referential_constraints`?\b"
                          r".*?constraint_schema\s*=\s*'([A-Za-z0-9_$]+)'"
                          r".*?table_name\s*=\s*'([A-Za-z0-9_$]+)'"
                          r".*?referenced_table_name\s+is\s+not\s+null", re.I | re.S),
     SVC_DICT_TABLE, "getTableForeignKeys", True),
    ("fk_kcu", re.compile(r"^\s*select\b.*?\bfrom\s+`?information_schema`?\s*\.\s*`?key_column_usage`?\b"
                          r".*?table_schema\s*=\s*'([A-Za-z0-9_$]+)'"
                          r".*?table_name\s*=\s*'([A-Za-z0-9_$]+)'"
                          r".*?referenced_table_name\s+is\s+not\s+null", re.I | re.S),
     SVC_DICT_TABLE, "getTableForeignKeys", True),
]

# GUI 클라이언트는 접속과 DB 전환 때마다 같은 메타 쿼리를 반복해서 던진다
# (SHOW VARIABLES 713행, SHOW TABLE STATUS, SHOW TRIGGERS ...). 왕복이 0.28초씩이라
# 이것만으로 탭 전환이 몇 초씩 걸리므로 결과를 캐시한다. 사용자 데이터 조회는 대상이 아니다.
# 스키마 구조(테이블 목록, 컬럼, 인덱스 등)는 캐시하지 않는다 — 최신 상태로 보여야 한다
# (사용자 지시 2026-08-13). 세션 중 사실상 변하지 않는 서버 설정만 캐시한다.
CACHE_RULES = [
    (re.compile(r"^\s*show\s+(variables|collation|engines|(character\s+set|charset))\b", re.I), 3600),
]
# 값이 매번 달라지는 것은 캐시하지 않는다 (위 규칙보다 우선한다).
RE_NO_CACHE = re.compile(r"^\s*(select\s+(now|curdate|curtime|connection_id|rand|uuid)\s*\(|"
                         r"show\s+(global\s+|session\s+)?status\b|"
                         r"show\s+(full\s+)?processlist\b)", re.I)


_LOG_FP = None
_COLOR = False
_LOG_DAY = None  # 마지막으로 구분선을 찍은 날짜
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# 처리 경로를 비용 순으로 물들인다. 화면을 훑을 때 비싼 요청이 먼저 눈에 들어오게.
#   회색 = 서버에 아예 안 감   초록 = dictionary 한 번   청록 = DDL 파싱
#   태그 없음(= 실제 SQL 실행)은 색을 주지 않아 오히려 도드라진다.
C_RESET = "\x1b[0m"
C_GRAY = "\x1b[90m"
C_GREEN = "\x1b[32m"
C_CYAN = "\x1b[36m"
C_YELLOW = "\x1b[33m"
SOURCE_COLOR = {
    "cache": C_GRAY, "map": C_GRAY,      # 왕복 0
    "empty": C_GREEN, "dict": C_GREEN,   # dictionary 한 번
    "ddl": C_CYAN,                       # CREATE TABLE 파싱
}


def enable_ansi():
    """색을 쓸 수 있는 환경인지 확인하고, Windows 콘솔이면 VT 처리를 켠다."""
    if os.environ.get("NO_COLOR"):
        return False
    if not sys.stderr.isatty():
        return False
    if os.name != "nt":
        return True
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-12)  # STD_ERROR_HANDLE
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))
    except Exception:
        return False


def paint(text, color):
    return f"{color}{text}{C_RESET}" if color else text


def _emit(text):
    """화면에는 색을 살려서, 로그 파일에는 색 코드를 빼고 남긴다."""
    print(text if _COLOR else ANSI_RE.sub("", text), file=sys.stderr, flush=True)
    if _LOG_FP:
        print(ANSI_RE.sub("", text), file=_LOG_FP, flush=True)


def log(*args):
    """한 줄을 시각과 함께 남긴다.

    시각은 HeidiSQL 이 자기 오류 주석에 쓰는 형식(`[13:23:07.954]`)에 맞춰 밀리초까지
    찍는다 - 클라이언트가 본 시각과 나란히 놓고 보는 일이 잦다. 날짜는 매 줄에 넣으면
    자리만 먹으므로 바뀔 때 구분선으로 알린다 (프록시를 며칠 띄워 두면 날을 넘는다).
    """
    global _LOG_DAY
    now = time.time()
    local = time.localtime(now)
    day = time.strftime("%Y-%m-%d", local)
    if day != _LOG_DAY:
        _LOG_DAY = day
        _emit(paint(f"---- {day} ----", C_GRAY))
    millis = int((now % 1) * 1000)
    stamp = time.strftime("%H:%M:%S", local) + f".{millis:03d}"
    _emit(paint(stamp, C_GRAY) + " " + " ".join(str(a) for a in args))


class QpError(Exception):
    """QueryPie 가 돌려준 오류. 클라이언트에는 MySQL ERR 패킷으로 전달된다."""


def humanize(exit_message):
    """qp.call 의 SystemExit 문자열을 클라이언트에 보낼 한 줄로 다듬는다.

    해석은 querypie_query.humanize_grpc 가 이미 해두므로, 여기서는 호출 위치
    같은 꼬리표만 떼어낸다. 옛 형식(message=<base64>)이 오면 그것도 해석한다.
    """
    text = (exit_message or "QueryPie 오류").strip()
    m = re.search(r"message=(\S+)", text)
    if m:
        return qp.humanize_grpc(m.group(1))
    return re.sub(r"^QueryPie 오류:\s*", "", text)


def session_expired(exc):
    """서버측 SQL 세션이 사라져서 난 오류인가 (다시 열면 풀린다)."""
    return "SessionNotFound" in humanize(str(exc))


# --------------------------------------------------------------------------
# QueryPie 세션 (프로세스당 하나, 클라이언트들이 공유하므로 락으로 직렬화)
# --------------------------------------------------------------------------
class QuerySession:
    def __init__(self, conn_name, max_rows, insecure=False,
                 lob_max_bytes=1 << 20, lob_cells=200, use_dictionary=True,
                 experimental_ddl=False, prefer_writer=False):
        self.lock = threading.Lock()
        self.window_id = secrets.token_hex(16)
        self.max_rows = max_rows
        self.insecure = insecure
        self.lob_max_bytes = lob_max_bytes
        self.lob_cells = lob_cells
        self.use_dictionary = use_dictionary
        self.experimental_ddl = experimental_ddl
        self.current_db = None  # 서버측 세션이 현재 보고 있는 database
        self._local = threading.local()  # 스레드별 처리 경로 기록
        self._pool = None  # 테이블 속성 병렬 조회용 (스레드를 재사용해야 커넥션도 재사용된다)
        self._collations = None  # (받은 시각, {db: 기본 collation})
        self.ready = False  # QueryPie 세션이 열렸는지 (credential 이 없으면 미룬다)
        self.warm_on_ready = False  # 세션이 열린 뒤 헤더를 예열할지
        self._pk_cache = {}  # (schema, table) -> 기본키 컬럼 이름 집합
        self._meta_cache = {}  # (db, 정규화한 SQL) -> (저장 시각, (컬럼, 행))
        self._header_cache = {}  # EMPTY_CHECK_RULES 의 키 -> 서버에서 배운 컬럼 헤더
        self._ddl_cache = {}  # (db, table) -> DDL 파싱 결과 (experimental)
        self.conn_name = conn_name
        self.prefer_writer = prefer_writer  # 쓰기 문장을 통과시키는 모드면 읽기 엔드포인트로 가면 안 된다
        self.open_payload = None  # 커넥션 해석에 인증이 필요해 세션을 열 때로 미룬다
        self.assembled = False  # 조립본인지 (b64 폴백이면 인증 후 다시 조립한다)
        self._reopening = False  # 세션 재확립 중 (재귀 방지)
        self.conn = None

    # -- 저수준 호출 ------------------------------------------------------
    def _call(self, service, method, payload, retry_expired=True):
        """qp.call 래핑. qp 는 실패 시 프로세스를 끝내므로 예외로 바꿔 프록시를 살린다.

        오래 쉬면 서버가 세션을 정리해 `SessionNotFound` 가 온다. 클라이언트는 그 사이
        아무것도 하지 않았으니 오류를 볼 이유가 없어, 세션을 다시 열고 한 번 더 보낸다.
        """
        try:
            return qp.call(service, method, payload, self.insecure, window_id=self.window_id)
        except SystemExit as e:
            if retry_expired and session_expired(e) and not self._reopening:
                self.reopen()
                return self._call(service, method, payload, retry_expired=False)
            raise QpError(humanize(str(e)))

    def reopen(self):
        """만료된 세션을 다시 연다. 보고 있던 database 도 그대로 되돌린다."""
        db = self.current_db
        self._reopening = True
        log("[proxy] 세션이 만료되어 다시 엽니다")
        try:
            (qp._refresh_tokens(self.insecure, self.window_id)
             or qp.login(self.insecure, self.window_id))
            self.open()
            if db:
                self.change_db(db)
        except QpError as e:
            raise QpError(f"세션을 다시 열지 못했습니다: {e}")
        finally:
            self._reopening = False

    def resolve(self):
        """커넥션 이름 → open 페이로드. 클러스터 엔드포인트를 그 자리에서 조립한다.

        조회 API 를 쓰므로 로그인 뒤에야 가능하다. --login-from-client 모드에서는 첫
        클라이언트 접속 전까지 계정을 모르니 여기까지 미뤄지고, 그동안 b64 파일로
        폴백했다면 인증이 생긴 뒤 다시 조립을 시도한다 (파일은 노드를 박제한 것이라
        엔드포인트로 여는 편이 낫다).
        """
        if self.open_payload is not None and self.assembled:
            return
        payload, src = qp.resolve_open_payload(self.conn_name, self.insecure, self.window_id,
                                               allow_file=False,
                                               prefer_writer=self.prefer_writer)
        assembled = payload is not None
        if payload is None:
            payload, name = qp.load_open(self.conn_name)
            src = f"파일 {name}"
        if payload is None:
            if self.open_payload is not None:
                return  # 앞서 잡아 둔 폴백이라도 쓴다
            raise QpError(f"'{self.conn_name}' 에 맞는 커넥션이 없습니다 "
                          f"(querypie_conn.py list 로 이름을 확인하세요)")
        self.open_payload, self.assembled = payload, assembled
        self.conn = qp.extract_field_raw(payload, 2).decode("utf-8", "replace")
        log(f"[proxy] 커넥션 {src}")

    def open(self):
        self.resolve()
        self._call("engine.session.SessionService", "open", self.open_payload)
        self.current_db = None
        self.ready = True
        log("[proxy] QueryPie 세션 열림")

    def ensure_ready(self):
        """세션이 아직 안 열렸으면 지금 연다 (credential 을 클라이언트에서 받는 모드용).

        인증 정보가 없으면 시작 시점에는 세션을 열 수 없어, 첫 접속이 끝난 뒤로 미룬다.
        """
        if self.ready:
            return True
        try:
            self.open()
        except (QpError, SystemExit) as e:
            log(f"[proxy] 세션을 열지 못했습니다: {e}")
            return False
        if self.warm_on_ready:
            self.warm_on_ready = False
            threading.Thread(target=self.warm_headers, daemon=True).start()
        return True

    def change_db(self, name):
        self._call(SVC_TX, "changeDatabase", qp.f_str(2, self.conn) + qp.f_str(3, name))
        self.current_db = name

    # -- 쿼리 -------------------------------------------------------------
    def _run(self, sql, db):
        """parse → execute → getDataTable. (컬럼명, 행) 반환.

        결과 행이 0건이면 QueryPie 는 getDataTable 을 SqlResultsetNotFound 로 거부한다.
        이때는 execute 응답에 담긴 컬럼 메타로 빈 결과셋을 만들어 돌려준다 — 클라이언트가
        스키마 조회(SHOW TRIGGERS, information_schema 등)에서 오류를 보지 않게 하는 처리다.
        """
        h = secrets.token_hex(16)
        oid = f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
        sql_id, parser = "", None
        for d in qp.sql_parse(self.conn, db, sql, oid, 1, self.insecure, self.window_id):
            pr = qp.extract_field_raw(d, 3)
            if pr:
                parser = pr
                m = re.search(rb"0H[0-9A-Z]{8,}", pr)
                if m:
                    sql_id = m.group().decode()
        if not (parser and sql_id):
            raise QpError("parse 응답에서 sqlId 를 찾지 못했습니다")
        ex = qp.sql_execute(self.conn, db, sql, parser, 1, self.max_rows,
                            self.insecure, self.window_id)
        cols = parse_execute_columns(ex)
        try:
            frames = qp.sql_get_data_table(self.conn, db, sql_id, 0, self.max_rows,
                                           "", self.insecure, self.window_id)
        except SystemExit as e:
            msg = humanize(str(e))
            if "SqlResultsetNotFound" not in msg:
                raise QpError(msg)
            return cols, []
        names, rows = parse_data_table(frames)
        if not cols:  # 메타를 못 얻으면 결과 헤더만으로 구성한다
            cols = [(n, "System.String", False) for n in names]
        return self.mark_primary_keys(cols, sql, db), self.expand_lobs(rows, db)

    def mark_primary_keys(self, cols, sql, db):
        """결과 컬럼에 기본키 표시를 채운다.

        QueryPie 가 주는 PK 메타(#8)는 결과가 커지면 사라진다 (같은 쿼리라도
        LIMIT 1 이면 붙고 262행이면 안 붙는 것을 확인했다). 그래서 대상 테이블의
        기본키를 SHOW KEYS 로 직접 조회해 보완한다. 테이블별로 한 번만 조회한다.
        """
        if any(c[2] for c in cols) or not RE_SELECT.match(sql):
            return cols  # 이미 표시됐거나, 테이블을 특정할 수 없는 문장
        schema, table = extract_table(sql)
        if not table:
            return cols
        pks = self.primary_keys(schema, table, db)
        if not pks:
            return cols
        return [(n, t, n in pks or is_pk) for n, t, is_pk in cols]

    def dict_call(self, method, db, table=None):
        """dictionary API 호출 (요청 필드는 #2 conn / #3 database / #4 table)."""
        payload = qp.f_str(2, self.conn) + qp.f_str(3, db or self.current_db or "")
        if table:
            payload += qp.f_str(4, table)
        return self._call(SVC_DICT_TABLE, method, payload)

    def primary_keys(self, schema, table, db):
        """대상 테이블의 기본키 컬럼 이름 집합 (실패하면 빈 집합).

        SHOW KEYS(약 280ms) 대신 getTableIndexes(약 40ms)를 쓴다. 응답은 인덱스마다
        {#1 uuid, #2 이름, 반복 #3 {#1 순번, #2 컬럼}, #4 종류} 이고 기본키는 이름이 PRIMARY 다.
        """
        key = (schema or db or self.current_db or "", table)
        if key in self._pk_cache:
            return self._pk_cache[key]
        pks = self._pk_via_dictionary(schema, db, table) if self.use_dictionary else None
        if pks is None:
            pks = self._pk_via_sql(schema, db, table)
        self._pk_cache[key] = pks
        return pks

    def _pk_via_dictionary(self, schema, db, table):
        """getTableIndexes 로 기본키 조회 (실패하면 None — 호출자가 SQL 로 넘어간다)."""
        pks = set()
        try:
            for d in self.dict_call("getTableIndexes", schema or db, table):
                for fn, entry in qp.iter_fields(d):
                    if fn != 1:
                        continue
                    # 인덱스 하나가 message 로 한 번 더 감싸여 오는 경우가 있다
                    if qp.find_field(qp.decode_raw(entry), 2, "str") is None:
                        inner = qp.extract_field_raw(entry, 1)
                        entry = inner if inner else entry
                    items = qp.decode_raw(entry)
                    if qp.find_field(items, 2, "str") != "PRIMARY":
                        continue
                    for fn2, col in qp.iter_fields(entry):
                        if fn2 == 3:
                            nm = qp.find_field(qp.decode_raw(col), 2, "str")
                            if nm:
                                pks.add(nm)
        except (QpError, SystemExit) as e:
            log(f"[proxy] dictionary 기본키 조회 실패({table}), SHOW KEYS 로 시도: {e}")
            return None
        return pks

    def _pk_via_sql(self, schema, db, table):
        """SHOW KEYS 로 기본키 조회 (dictionary 를 쓰지 않거나 실패했을 때)."""
        query = f"SHOW KEYS FROM `{table}`" + (f" FROM `{schema}`" if schema else "")
        try:
            # SHOW 문이라 이 경로가 다시 여기로 들어오지는 않는다 (재귀 없음).
            kcols, krows = self._run(query, db)
            idx = {c[0].lower(): i for i, c in enumerate(kcols)}
            kn, cn = idx.get("key_name"), idx.get("column_name")
            if kn is None or cn is None:
                return set()
            return {r[cn] for r in krows if r[kn] == "PRIMARY"}
        except (QpError, SystemExit, IndexError) as e:
            log(f"[proxy] 기본키 조회 실패({table}): {e}")
            return set()

    def expand_lobs(self, rows, db):
        """행 안의 LOB 핸들을 실제 값으로 바꾼다 (구현은 querypie_query 와 공유한다)."""
        return qp.expand_lobs(rows, self.conn, db or self.current_db or "",
                              self.lob_cells, self.lob_max_bytes,
                              self.insecure, self.window_id,
                              notify=lambda msg: log(f"[proxy] {msg}"))
        return rows

    def try_dictionary(self, sql, db):
        """dictionary API 로 바로 답할 수 있는 문장이면 (컬럼, 행) 을, 아니면 None.

        결과 형태가 SQL 응답과 정확히 같은 것만 다룬다 — 컬럼 구성을 우리가 지어내면
        클라이언트가 스키마를 잘못 읽기 때문이다. SHOW FULL COLUMNS/TABLE STATUS 는
        dictionary 응답에 없는 컬럼(타입, 행 수 등)이 많아 대상에서 뺐다.
        """
        m = RE_SHOW_TABLES.match(sql)
        if m:
            target = m.group(1) or db or self.current_db
            if not target:
                return None
            # SHOW TABLES 는 테이블과 뷰를 모두 돌려준다. dictionary 는 둘을 나눠 주므로
            # 합쳐야 한다 (getTables 만 쓰면 sys 스키마에서 뷰 100개가 통째로 빠진다).
            names = []
            for method, svc in (("getTables", SVC_DICT_TABLE), ("getViews", SVC_DICT_VIEW)):
                payload = qp.f_str(2, self.conn) + qp.f_str(3, target)
                for d in self._call(svc, method, payload):
                    names += [v.decode("utf-8", "replace")
                              for fn, v in qp.iter_fields(d) if fn == 1]
            self._local.source = "dict"
            return ([(f"Tables_in_{target}", "System.String", False)],
                    [[n] for n in sorted(set(names))])

        if RE_SHOW_DATABASES.match(sql):
            names = []
            for d in self._call(SVC_DICT_DB, "getDatabases",
                                qp.f_str(2, self.conn) + qp.f_str(3, db or self.current_db or "")):
                names += [v.decode("utf-8", "replace") for fn, v in qp.iter_fields(d) if fn == 1]
            if names:
                self._local.source = "dict"
                return [("Database", "System.String", False)], [[n] for n in names]
            return None

        m = RE_SCHEMA_COLLATION.match(sql)
        if m:
            header = self._header_cache.get("schema_collation")
            if header:
                fast = self.schema_collation(m.group(1), [h[0] for h in header])
                if fast is not None:
                    self._local.source = "map"
                    return fast

        if self.experimental_ddl:
            m = RE_CHECK_CONSTRAINTS.match(sql)
            if m:
                header = self._header_cache.get(header_key("check_constraints", sql))
                if header and self.has_check_constraint(m.group(1), m.group(2)) is False:
                    self._local.source = "ddl"
                    return header, []

        m = RE_SHOW_TABLE_STATUS.match(sql)
        if m:
            target = m.group(1) or db or self.current_db
            header = self._header_cache.get("table_status")
            if target and header:
                fast = self.table_status(target, [h[0] for h in header], m.group(2))
                if fast is not None:
                    self._local.source = "dict"
                    return fast

        if self.experimental_ddl:
            m = RE_SHOW_KEYS.match(sql)
            if m:
                # `db`.`t` 면 (db, t, None), `t` FROM `db` 면 (t, None, db)
                g1, g2, g3 = m.groups()
                schema, table = (g1, g2) if g2 else (g3, g1)
                fast = self.show_keys_from_ddl(schema or db, table)
                if fast is not None:
                    self._local.source = "ddl"
                    return fast

            m = RE_IS_COLUMNS.match(sql)
            if m:
                header = self._header_cache.get(header_key("is_columns", sql))
                if header:
                    fast = self.is_columns_from_ddl(m.group(1), m.group(2), [h[0] for h in header])
                    if fast is not None:
                        self._local.source = "ddl"
                        return fast

        empty = self.try_empty_check(sql, db)
        if empty is not None:
            self._local.source = "empty"
            return empty

        m = RE_SHOW_CREATE.match(sql)
        if m:
            schema, table = (m.group(1), m.group(2)) if m.group(2) else (None, m.group(1))
            target = schema or db or self.current_db
            if not target:
                return None
            script = ""
            for d in self.dict_call("getTableScript", target, table):
                raw = qp.extract_field_raw(d, 1)
                if raw:
                    script = raw.decode("utf-8", "replace")
            if not script:
                return None
            self._local.source = "dict"
            return ([("Table", "System.String", False), ("Create Table", "System.String", False)],
                    [[table, script]])
        return None

    def ddl_info(self, db, table):
        """CREATE TABLE 을 sqlglot 으로 파싱해 인덱스와 컬럼 정보를 얻는다 (테이블별 캐시).

        dictionary 만으로는 알 수 없는 것을 DDL 이 갖고 있다:
          - 인덱스가 UNIQUE 인지 (getTableIndexes 에는 그 구분이 없다)
          - 기본값이 빈 문자열인지 아예 없는지 (dictionary 는 둘 다 빈 문자열로 준다)
        """
        # GUI 는 테이블 하나를 열 때 SHOW KEYS 와 컬럼 조회를 연달아 던진다. 그 구간만
        # 묶으면 getTableScript 왕복이 한 번으로 줄고, 스키마 변경도 1초면 반영된다.
        key = (db or self.current_db or "", table)
        hit = self._ddl_cache.get(key)
        if hit and time.time() - hit[0] < DDL_CACHE_TTL:
            return hit[1]
        info = None
        try:
            script = ""
            for d in self.dict_call("getTableScript", db, table):
                raw = qp.extract_field_raw(d, 1)
                if raw:
                    script = raw.decode("utf-8", "replace")
            if script:
                info = parse_ddl(script)
        except (QpError, SystemExit) as e:
            log(f"[proxy] DDL 조회 실패({table}): {e}")
        except Exception as e:  # 파서가 못 읽는 구문이면 조용히 포기하고 SQL 로 돌아간다
            log(f"[proxy] DDL 파싱 실패({table}), SQL 로 처리합니다: {e}")
        self._ddl_cache[key] = (time.time(), info)
        return info

    def schema_collation(self, db, header):
        """DB 기본 collation 을 맵에서 답한다 (모르는 DB 면 None -> SQL).

        하나만 묻든 전부 묻든 서버 비용이 같아, 한 번에 받아 두고 나눠 쓴다.
        """
        cached = self._collations
        if cached is None or time.time() - cached[0] >= COLLATION_MAP_TTL:
            mapping = {}
            cols, rows = self._exec_locked(
                "SELECT SCHEMA_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA", db)
            for r in rows:
                if len(r) >= 2 and r[0]:
                    mapping[r[0]] = r[1]
            if not mapping:
                return None
            self._collations = cached = (time.time(), mapping)
            log(f"[proxy] DB 기본 collation {len(mapping)}건 적재")
        value = cached[1].get(db)
        if value is None:
            return None  # 새로 만들어진 DB 등 -> SQL 로 확인
        return [(header[0], "System.String", False)], [[value]]

    def has_check_constraint(self, db, table):
        """CHECK 제약이 있는지 (모르면 None)."""
        info = self.ddl_info(db, table)
        return None if info is None else info.get("has_check", False)

    def _exec_locked(self, sql, db):
        """세션 락을 잡고 SQL 을 실행한다 (내부 조회용)."""
        with self.lock:
            return self._exec(sql, db)

    def table_status(self, db, header, like=None):
        """SHOW TABLE STATUS [LIKE 'x'] 를 getTableAttributes 병렬 호출로 만든다.

        테이블마다 왕복이 한 번이라 개수가 많으면 SQL 한 번이 낫다. 뷰가 섞인 경우도
        속성 표현을 확인하지 못했으므로 SQL 로 넘긴다 (LIKE 로 걸러낸 뒤 판단한다 —
        그래야 뷰가 있는 DB 에서도 테이블 하나만 묻는 질의는 빠르게 답할 수 있다).
        """
        tables, views = [], []
        for method, svc, sink in (("getTables", SVC_DICT_TABLE, tables),
                                  ("getViews", SVC_DICT_VIEW, views)):
            payload = qp.f_str(2, self.conn) + qp.f_str(3, db)
            for d in self._call(svc, method, payload):
                sink += [v.decode("utf-8", "replace") for fn, v in qp.iter_fields(d) if fn == 1]
        if like is not None:
            matcher = like_to_regex(like)
            tables = [t for t in tables if matcher.match(t)]
            views = [v for v in views if matcher.match(v)]
        if views or not tables or len(tables) > TABLE_STATUS_MAX_TABLES:
            return None
        if any(h not in SHOW_TABLE_STATUS_MAP for h in header):
            return None  # 서버가 우리가 모르는 컬럼을 준다

        def fetch(table):
            out = {}
            for d in self.dict_call("getTableAttributes", db, table):
                for fn, v in qp.iter_fields(d):
                    if fn != 1:
                        continue
                    it = qp.decode_raw(v)
                    out[qp.find_field(it, 1, "str")] = qp.find_field(it, 2, "str")
            return out

        attrs = list(self.pool.map(fetch, sorted(tables)))
        rows = []
        for a in attrs:
            row = []
            for h in header:
                v = a.get(SHOW_TABLE_STATUS_MAP[h])
                if h.endswith("_time"):
                    v = to_sql_datetime(v)
                elif v is None and h in TABLE_STATUS_EMPTY_STRING:
                    v = ""  # 서버는 빈 문자열로, dictionary 는 값 없음으로 준다
                row.append(v)
            rows.append(row)
        return [(h, "System.String", False) for h in header], rows

    def table_structure(self, db, table):
        """getTableStructure → [{헤더: 값}] (컬럼 순서 유지)."""
        for d in self.dict_call("getTableStructure", db, table):
            body = qp.extract_field_raw(d, 1) or d
            hdr = [v.decode("utf-8", "replace") for fn, v in qp.iter_fields(body) if fn == 4]
            if not hdr:
                continue
            out = []
            for fn, v in qp.iter_fields(body):
                if fn != 5:
                    continue
                vals = [x.decode("utf-8", "replace") for f2, x in qp.iter_fields(v) if f2 == 1]
                out.append(dict(zip(hdr, vals)))
            return out
        return []

    def is_columns_from_ddl(self, db, table, header):
        """information_schema.COLUMNS 결과를 재구성한다 (못 만들면 None).

        값은 세 곳에서 모은다.
          - getTableStructure: 타입, 키, 코멘트, 문자셋 등 대부분
          - CREATE TABLE 파싱: 기본값 (dictionary 는 빈 문자열과 없음을 구분하지 못한다)
          - column_metrics: 길이, 정밀도 (타입에서 계산)
        """
        rows_src = self.table_structure(db, table)
        info = self.ddl_info(db, table)
        if not rows_src or not info:
            return None
        rows = []
        try:
            for pos, c in enumerate(rows_src, 1):
                name = c.get("ColumnName", "")
                ddl_col = info["columns"].get(name)
                if ddl_col is None:
                    return None  # DDL 과 어긋나면 손대지 않는다
                cmax, coct, nprec, nscale, dtprec = column_metrics(
                    c.get("DataType"), c.get("ColumnType"), c.get("ColumnLength"),
                    c.get("CharacterSetClient"))
                values = {
                    "TABLE_CATALOG": "def",
                    "TABLE_SCHEMA": db,
                    "TABLE_NAME": table,
                    "COLUMN_NAME": name,
                    "ORDINAL_POSITION": str(pos),
                    "COLUMN_DEFAULT": unquote_default(ddl_col["default"]),
                    "IS_NULLABLE": "YES" if c.get("Nullable") == "1" else "NO",
                    "DATA_TYPE": c.get("DataType", ""),
                    "CHARACTER_MAXIMUM_LENGTH": cmax,
                    "CHARACTER_OCTET_LENGTH": coct,
                    "NUMERIC_PRECISION": nprec,
                    "NUMERIC_SCALE": nscale,
                    "DATETIME_PRECISION": dtprec,
                    "CHARACTER_SET_NAME": c.get("CharacterSetClient") or None,
                    "COLLATION_NAME": c.get("CollationConnection") or None,
                    "COLUMN_TYPE": c.get("ColumnType", ""),
                    "COLUMN_KEY": c.get("ColumnKey", ""),
                    "EXTRA": c.get("EXTRA", ""),
                    "PRIVILEGES": COLUMN_PRIVILEGES,
                    "COLUMN_COMMENT": c.get("Comment", ""),
                    "GENERATION_EXPRESSION": "",
                    "SRS_ID": None,
                }
                if any(h not in values for h in header):
                    return None  # 서버가 우리가 모르는 컬럼을 준다 -> 손대지 않는다
                rows.append([values[h] for h in header])
        except ValueError as e:
            log(f"[proxy] COLUMNS 재구성 포기({table}), SQL 로 처리합니다: {e}")
            return None
        return [(h, "System.String", False) for h in header], rows

    def show_keys_from_ddl(self, db, table):
        """DDL 파싱 결과로 SHOW KEYS 결과를 만든다 (못 만들면 None).

        Cardinality 는 통계값이라 DDL 에 없다. NULL 로 두므로, 그 값을 쓰는 클라이언트가
        있다면 --no-experimental-ddl 로 꺼야 한다.
        """
        info = self.ddl_info(db, table)
        if not info or not info["indexes"]:
            return None
        rows = []
        for idx in info["indexes"]:
            for seq, (col, prefix) in enumerate(idx["columns"], 1):
                nullable = info["columns"].get(col, {}).get("nullable")
                rows.append([
                    table,
                    "0" if idx["unique"] else "1",
                    idx["name"],
                    str(seq),
                    col,
                    "A",
                    None,                      # Cardinality: DDL 에 없다
                    str(prefix) if prefix else None,
                    None,                      # Packed
                    "YES" if nullable else "",
                    idx["using"] or "BTREE",
                    "", "",                    # Comment, Index_comment
                    "YES",                     # Visible
                    None,                      # Expression
                ])
        cols = [(n, "System.String", False) for n in SHOW_KEYS_COLUMNS]
        return cols, rows

    def warm_headers(self):
        """빈 결과 판정에 쓸 컬럼 헤더를 미리 배워 둔다.

        학습 전에는 SQL 로 넘기므로, 클라이언트가 붙기 전에 배워 두면 첫 접속부터
        dictionary 로 답할 수 있다. SQL 문구는 HeidiSQL 이 실제로 던지는 형태와 같아야
        한다 — 헤더 캐시 키가 SELECT 절을 포함하기 때문이다(header_key 참고).
        """
        db = self.current_db
        table = None
        try:
            if not db:
                for d in self._call(SVC_DICT_DB, "getDatabases",
                                    qp.f_str(2, self.conn) + qp.f_str(3, "")):
                    names = [v.decode("utf-8", "replace")
                             for fn, v in qp.iter_fields(d) if fn == 1]
                    if names:
                        db = names[0]
                        break
            if not db:
                return
            for d in self.dict_call("getTables", db):
                names = [v.decode("utf-8", "replace") for fn, v in qp.iter_fields(d) if fn == 1]
                if names:
                    table = names[0]
                    break
        except (QpError, SystemExit) as e:
            log(f"[proxy] 헤더 예열 준비 실패: {e}")
            return

        warmups = [
            f"SHOW TRIGGERS FROM `{db}`",
            f"SHOW FUNCTION STATUS WHERE LOWER(Db) = LOWER('{db}')",
            f"SHOW PROCEDURE STATUS WHERE LOWER(Db) = LOWER('{db}')",
            f"SELECT *, EVENT_SCHEMA AS `Db`, EVENT_NAME AS `Name` "
            f"FROM INFORMATION_SCHEMA.`EVENTS` WHERE EVENT_SCHEMA='{db}'",
            f"SHOW TABLE STATUS FROM `{db}`",
            f"SELECT `DEFAULT_COLLATION_NAME` FROM `information_schema`.`SCHEMATA` "
            f"WHERE `SCHEMA_NAME`='{db}'",
        ]
        if table:
            warmups += [
                f"SELECT * FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE "
                f"CONSTRAINT_SCHEMA='{db}' AND TABLE_NAME='{table}' "
                f"AND REFERENCED_TABLE_NAME IS NOT NULL",
                f"SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE "
                f"TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' "
                f"AND REFERENCED_TABLE_NAME IS NOT NULL",
            ]
            if self.experimental_ddl:
                # COLUMNS, CHECK 제약도 학습된 헤더가 있어야 동작한다. 문구는 HeidiSQL 이
                # 던지는 형태와 같아야 한다 (헤더 키가 SELECT 절을 포함하므로).
                warmups.append(
                    f"SELECT * FROM `information_schema`.`COLUMNS` "
                    f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' "
                    f"ORDER BY ORDINAL_POSITION")
                warmups.append(
                    f"SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE "
                    f"FROM `information_schema`.`CHECK_CONSTRAINTS` AS cc, "
                    f"`information_schema`.`TABLE_CONSTRAINTS` AS tc "
                    f"WHERE tc.CONSTRAINT_SCHEMA='{db}' AND tc.TABLE_NAME='{table}' "
                    f"AND tc.CONSTRAINT_TYPE='CHECK' "
                    f"AND tc.CONSTRAINT_SCHEMA=cc.CONSTRAINT_SCHEMA "
                    f"AND tc.CONSTRAINT_NAME=cc.CONSTRAINT_NAME")
        t = time.time()
        for sql in warmups:
            try:
                self.query(sql, db)
            except (QpError, SystemExit) as e:
                log(f"[proxy] 헤더 예열 중 오류(무시): {e}")
        log(f"[proxy] 헤더 예열 완료 {len(self._header_cache)}종 ({time.time() - t:.1f}s)")

    def try_empty_check(self, sql, db):
        """'없는 것을 확인하는' 조회면 dictionary 로 판정해 빈 결과를 돌려준다.

        - dictionary 가 "있다" 고 하면 None 을 돌려 SQL 로 넘긴다 (값은 SQL 이 정확하다)
        - 헤더를 아직 배우지 않았어도 None (그 SQL 실행 결과로 헤더를 배운다)
        """
        for key, pattern, svc, method, needs_table in EMPTY_CHECK_RULES:
            m = pattern.match(sql)
            if not m:
                continue
            header = self._header_cache.get(header_key(key, sql))
            if header is None:
                return None
            target_db = m.group(1) or db or self.current_db
            payload = qp.f_str(2, self.conn) + qp.f_str(3, target_db)
            if needs_table:
                payload += qp.f_str(4, m.group(2))
            frames = self._call(svc, method, payload)
            if any(len(f) for f in frames):
                return None  # 실제로 있다 -> SQL 로 정확히 조회
            return header, []
        return None

    def learn_header(self, sql, cols, rows):
        """조회 결과의 컬럼 헤더를 기억해 둔다 (다음부터 dictionary 로 대체).

        헤더는 행 수와 무관하게 같으므로 행이 있는 결과에서도 배운다.
        """
        if not cols:
            return
        if RE_SCHEMA_COLLATION.match(sql):
            if "schema_collation" not in self._header_cache:
                self._header_cache["schema_collation"] = cols
                log(f"[proxy] 'schema_collation' 헤더 학습 ({len(cols)}컬럼)")
            return
        if self.experimental_ddl and RE_CHECK_CONSTRAINTS.match(sql):
            full = header_key("check_constraints", sql)
            if full not in self._header_cache:
                self._header_cache[full] = cols
                log(f"[proxy] 'check_constraints' 헤더 학습 ({len(cols)}컬럼)")
            return
        if RE_SHOW_TABLE_STATUS.match(sql):
            if "table_status" not in self._header_cache:
                self._header_cache["table_status"] = cols
                log(f"[proxy] 'table_status' 헤더 학습 ({len(cols)}컬럼)")
            return
        if self.experimental_ddl and RE_IS_COLUMNS.match(sql):
            full = header_key("is_columns", sql)
            if full not in self._header_cache:
                self._header_cache[full] = cols
                log(f"[proxy] 'is_columns' 헤더 학습 ({len(cols)}컬럼)")
            return
        for key, pattern, _, _, _ in EMPTY_CHECK_RULES:
            if not pattern.match(sql):
                continue
            full = header_key(key, sql)
            if full not in self._header_cache:
                self._header_cache[full] = cols
                log(f"[proxy] '{key}' 헤더 학습 ({len(cols)}컬럼) — 다음부터 dictionary 로 답합니다")
            return

    @property
    def pool(self):
        """병렬 조회용 스레드 풀. 호출마다 새로 만들면 스레드가 바뀌어
        커넥션을 다시 맺느라(TLS 핸드셰이크) 병렬 이득이 사라진다."""
        if self._pool is None:
            self._pool = cf.ThreadPoolExecutor(max_workers=TABLE_STATUS_WORKERS,
                                               thread_name_prefix="qp-attr")
        return self._pool

    @property
    def last_source(self):
        """직전 query() 를 어느 경로로 처리했는지 (로그 표시용).

        클라이언트 커넥션마다 스레드가 따로라 스레드 지역에 둔다.
        """
        return getattr(self._local, "source", "sql")

    def query(self, sql, db):
        """세션 DB 를 맞춘 뒤 쿼리 실행. 세션이 끊겼으면 한 번 다시 열고 재시도한다."""
        self._local.source = "sql"
        if self.use_dictionary:
            try:
                fast = self.try_dictionary(sql, db)
            except (QpError, SystemExit) as e:
                log(f"[proxy] dictionary 조회 실패, SQL 로 처리합니다: {e}")
                fast = None
            if fast is not None:
                return fast
            self._local.source = "sql"  # 변환을 시도했지만 못 쓴 경우

        ttl = cache_ttl(sql)
        key = (db or self.current_db or "", " ".join(sql.split()))
        if ttl:
            hit = self._meta_cache.get(key)
            if hit and time.time() - hit[0] < ttl:
                cols, rows = hit[1]
                self._local.source = "cache"
                return cols, [r[:] for r in rows]  # 호출자가 행을 고쳐도 캐시가 상하지 않게
        with self.lock:
            try:
                result = self._exec(sql, db)
            except QpError as e:
                if "SessionNotFound" not in str(e) and "Session" not in str(e):
                    raise
                log("[proxy] 세션이 끊겨 다시 엽니다")
                self.open()
                result = self._exec(sql, db)
        if self.use_dictionary:
            self.learn_header(sql, result[0], result[1])
        if ttl:
            # 캐시에는 사본을 둔다. 호출자가 받은 행을 고쳐도 다음 조회가 오염되지 않는다.
            self._meta_cache[key] = (time.time(), (result[0], [r[:] for r in result[1]]))
        return result

    def clear_cache(self):
        """스키마를 바꾸는 문장이 지나갔을 때 캐시를 버린다."""
        if self._meta_cache:
            self._meta_cache.clear()
            self._pk_cache.clear()
            log("[proxy] 메타 캐시 비움")

    def _exec(self, sql, db):
        """세션이 만료됐으면 다시 열고 한 번만 더 시도한다 (_call 과 같은 사정이다)."""
        for attempt in (0, 1):
            if db and db != self.current_db:
                self.change_db(db)
            try:
                return self._run(sql, db or self.current_db or "")
            except SystemExit as e:  # sql_parse/execute 는 qp.call 을 직접 쓴다
                if attempt == 0 and session_expired(e) and not self._reopening:
                    self.reopen()
                    continue
                raise QpError(humanize(str(e)))


# information_schema.COLUMNS 의 길이, 정밀도 컬럼을 타입에서 계산하는 규칙.
# 운영 중인 MySQL 스키마 여러 벌에서 관측한 값으로 만들었다.
# 여기 없는 타입을 만나면 값을 지어내지 않고 통째로 SQL 로 넘긴다.
INT_PRECISION = {"tinyint": 3, "smallint": 5, "mediumint": 7, "int": 10, "bigint": 19}
INT_PRECISION_UNSIGNED = {"tinyint": 3, "smallint": 5, "mediumint": 7, "int": 10, "bigint": 20}
# 문자 하나가 차지하는 최대 바이트 (CHARACTER_OCTET_LENGTH 계산용)
CHARSET_BYTES = {"ascii": 1, "latin1": 1, "utf8": 3, "utf8mb3": 3, "utf8mb4": 4, "binary": 1}
# 길이가 타입으로 고정된 것들 (문자 수 = 바이트 수)
FIXED_LOB_LENGTH = {
    "tinytext": 255, "text": 65535, "mediumtext": 16777215, "longtext": 4294967295,
    "tinyblob": 255, "blob": 65535, "mediumblob": 16777215, "longblob": 4294967295,
}
TEMPORAL_TYPES = {"datetime", "time", "timestamp"}
NO_LENGTH_TYPES = {"date", "json", "year", "geometry"}
FLOAT_PRECISION = {"float": 12, "double": 22}


def column_metrics(data_type, column_type, char_len, charset):
    """(CHAR_MAX, CHAR_OCT, NUM_PRECISION, NUM_SCALE, DATETIME_PRECISION).

    규칙이 없는 타입이면 ValueError 를 던져 호출자가 SQL 로 돌아가게 한다
    (enum, set, decimal 처럼 계산이 까다롭거나 아직 관측하지 못한 타입).
    """
    t = (data_type or "").lower()
    unsigned = "unsigned" in (column_type or "").lower()
    if t in INT_PRECISION:
        table = INT_PRECISION_UNSIGNED if unsigned else INT_PRECISION
        return None, None, str(table[t]), "0", None
    if t == "bit":
        m = re.search(r"\((\d+)\)", column_type or "")
        return None, None, m.group(1) if m else "1", None, None
    if t in FLOAT_PRECISION:
        return None, None, str(FLOAT_PRECISION[t]), None, None
    if t in FIXED_LOB_LENGTH:
        n = str(FIXED_LOB_LENGTH[t])
        return n, n, None, None, None
    if t in ("char", "varchar"):
        if not char_len:
            raise ValueError(f"{t} 의 길이를 알 수 없습니다")
        mult = CHARSET_BYTES.get((charset or "").lower())
        if mult is None:
            raise ValueError(f"charset '{charset}' 의 바이트 수를 모릅니다")
        return str(char_len), str(int(char_len) * mult), None, None, None
    if t in TEMPORAL_TYPES:
        m = re.search(r"\((\d+)\)", column_type or "")
        return None, None, None, None, m.group(1) if m else "0"
    if t in NO_LENGTH_TYPES:
        return None, None, None, None, None
    raise ValueError(f"타입 '{data_type}' 의 길이 규칙이 없습니다")


COLUMN_PRIVILEGES = "select,insert,update,references"


def unquote_default(value):
    """sqlglot 이 준 기본값 표현을 information_schema.COLUMN_DEFAULT 형태로 바꾼다.

    파서는 문자열 기본값을 따옴표째(`''`, `'N'`) 준다. 서버는 따옴표 없이 준다.
    """
    if value is None:
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1].replace("\\'", "'").replace('\\"', '"')
    # 따옴표 없는 NULL 은 "기본값 없음"이다. 문자열 'NULL' 이면 위에서 처리됐다.
    if value.upper() == "NULL":
        return None
    # 표현식 기본값(`DEFAULT (NOW())`)은 서버가 자기 방식으로 정규화해 돌려준다
    # (`now()`). 그 표기를 정확히 흉내낼 수 없으므로 이 테이블은 SQL 로 넘긴다.
    if value.startswith("("):
        raise ValueError(f"표현식 기본값 {value!r} 은 서버 표기를 알 수 없습니다")
    # 파서는 인자 없는 함수를 CURRENT_TIMESTAMP() 로 렌더링하지만 서버는 괄호 없이 준다.
    # 인자가 있는 CURRENT_TIMESTAMP(3) 은 서버도 괄호째 주므로 건드리지 않는다.
    return re.sub(r"\(\)$", "", value)


# SHOW TABLE STATUS 컬럼 → getTableAttributes 의 키.
SHOW_TABLE_STATUS_MAP = {
    "Name": "TableName", "Engine": "Engine", "Version": "Version",
    "Row_format": "RowFormat", "Rows": "TableRows", "Avg_row_length": "AvgRowLength",
    "Data_length": "DataLength", "Max_data_length": "MaxDataLength",
    "Index_length": "IndexLength", "Data_free": "DataFree",
    "Auto_increment": "AutoIncrement", "Create_time": "CreateTime",
    "Update_time": "UpdateTime", "Check_time": "CheckTime",
    "Collation": "TableCollation", "Checksum": "CHECKSUM",
    "Create_options": "CreateOptions", "Comment": "TableComment",
}
# 테이블 하나마다 왕복이 한 번이라, 많아지면 SQL 한 번이 더 빠르다.
# 실측(병렬 8스레드): 6개 63ms / 8개 84ms / 24개 234ms / 33개 262ms, SQL 은 약 300ms 고정.
TABLE_STATUS_MAX_TABLES = 30
TABLE_STATUS_WORKERS = 8
# 서버가 빈 문자열로 주는 컬럼. dictionary 는 값 없음으로 주므로 되돌려야 한다.
TABLE_STATUS_EMPTY_STRING = {"Create_options", "Comment"}

# DB 기본 collation 은 ALTER DATABASE 로만 바뀌므로 오래 들고 있어도 된다.
# 34개를 한 번에 받는 비용이 하나를 받는 비용과 같아(약 0.3초) 통째로 받아 둔다.
# 만료되면 다음에 필요할 때 다시 받는다 (주기 갱신은 하지 않는다).
COLLATION_MAP_TTL = 600.0


def like_to_regex(pattern):
    """MySQL LIKE 패턴을 정규식으로 바꾼다 (% = 여러 글자, _ = 한 글자).

    테이블명 비교는 대소문자를 가리지 않는 쪽으로 맞춘다.
    """
    out = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            out.append(re.escape(pattern[i + 1]))
            i += 2
            continue
        out.append(".*" if ch == "%" else ("." if ch == "_" else re.escape(ch)))
        i += 1
    return re.compile("^" + "".join(out) + "$", re.I)


def to_sql_datetime(value):
    """'05/28/2026 12:05:25' → '2026-05-28 12:05:25' (형식이 다르면 그대로)."""
    if not value:
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2}:\d{2})$", value)
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)} {m.group(4)}" if m else value


SHOW_KEYS_COLUMNS = [
    "Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation",
    "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment",
    "Index_comment", "Visible", "Expression",
]


def parse_ddl(script):
    """CREATE TABLE 문 → {'indexes': [...], 'columns': {이름: {...}}}.

    정규식 대신 sqlglot 으로 구문 트리를 읽는다. DDL 은 따옴표, 주석, 표현식 인덱스처럼
    정규식으로 다루기 어려운 요소가 많다.
    """
    tree = sqlglot.parse_one(script, dialect="mysql")
    exp = sqlglot_exp
    indexes, columns = [], {}

    def index_entry(node):
        """인덱스 노드 → (이름, [(컬럼명, prefix 길이)]).

        세 종류가 트리 모양이 다르다.
          PRIMARY KEY (`a`)          → expressions 에 컬럼
          UNIQUE `ux` (`a`)          → this 가 Schema, 그 안에 이름과 컬럼
          INDEX `ix` (`a`) USING ... → this 가 이름, expressions 에 컬럼
        """
        if isinstance(node, exp.PrimaryKey):
            return "PRIMARY", [column_of(x) for x in node.args.get("expressions", [])]
        if isinstance(node, exp.UniqueColumnConstraint):
            schema = node.this
            name = schema.this.name if schema is not None and schema.this is not None else ""
            items = schema.expressions if schema is not None else []
            return name, [column_of(x) for x in items]
        name = node.this.name if node.this is not None else ""
        return name, [column_of(x) for x in node.args.get("expressions", [])]

    def column_of(node):
        """(컬럼명, prefix 길이). `col`(10) 처럼 prefix 가 붙은 형태를 함께 읽는다."""
        text = node.sql(dialect="mysql")
        m = re.search(r"\((\d+)\)\s*$", text)
        prefix = int(m.group(1)) if m else None
        if isinstance(node, exp.Ordered):
            node = node.this
        name = node.name if getattr(node, "name", "") else ""
        return name, prefix

    schema = tree.this if isinstance(tree, exp.Create) else None
    if not isinstance(schema, exp.Schema):
        return {"indexes": [], "columns": {}, "has_check": False}

    for e in schema.expressions:
        if isinstance(e, exp.ColumnDef):
            default = e.find(exp.DefaultColumnConstraint)
            notnull = e.find(exp.NotNullColumnConstraint)
            columns[e.name] = {
                "type": e.args["kind"].sql(dialect="mysql") if e.args.get("kind") else "",
                "default": default.this.sql(dialect="mysql") if default and default.this else None,
                "nullable": notnull is None,
            }
        elif isinstance(e, (exp.PrimaryKey, exp.UniqueColumnConstraint,
                            exp.IndexColumnConstraint)):
            name, cols = index_entry(e)
            indexes.append({
                "name": name,
                "unique": not isinstance(e, exp.IndexColumnConstraint),
                "using": _index_using(e),
                "columns": cols,
            })

    # 파서가 컬럼명을 잘못 읽으면(예: prefix 인덱스에서 대소문자가 바뀌는 경우가 있다)
    # 결과가 조용히 틀리므로, 컬럼 정의에 없는 이름이 나오면 통째로 포기한다.
    for idx in indexes:
        for col, _ in idx["columns"]:
            if col not in columns:
                raise ValueError(f"인덱스 '{idx['name']}' 의 컬럼 '{col}' 을 "
                                 f"테이블 정의에서 찾지 못했습니다")
    has_check = any(True for _ in tree.find_all(exp.Check, exp.CheckColumnConstraint))
    return {"indexes": indexes, "columns": columns, "has_check": has_check}


def _index_using(node):
    m = re.search(r"\bUSING\s+(\w+)", node.sql(dialect="mysql"), re.I)
    return m.group(1).upper() if m else None


def header_key(rule_key, sql):
    """헤더 캐시 키. SELECT 는 고르는 컬럼이 곧 결과 헤더라 그 부분까지 키에 넣는다.

    같은 테이블을 봐도 `SELECT *` 와 `SELECT *, X AS Db` 는 컬럼 수가 다르다.
    구분하지 않으면 한쪽에서 배운 헤더를 다른 쪽에 잘못 돌려주게 된다.
    """
    if not RE_SELECT.match(sql):
        return rule_key
    head = re.split(r"\bfrom\b", sql, maxsplit=1, flags=re.I)[0]
    return f"{rule_key}|{' '.join(head.split()).lower()}"


def cache_ttl(sql):
    """이 문장의 결과를 몇 초 캐시할지 (0 이면 캐시하지 않음)."""
    if RE_NO_CACHE.match(sql):
        return 0
    for pattern, ttl in CACHE_RULES:
        if pattern.match(sql):
            return ttl
    return 0


def parse_execute_columns(frames):
    """execute 응답의 컬럼 메타 → [(이름, CLR 타입, PK 여부)].

    구조는 프레임 → #2 → 반복 #1 이고, 그중 #3 을 가진 항목이 컬럼 정의 묶음이다
    (#3 안에서 반복 #5 = {#1 순번, #2 이름, #3 CLR 타입, #8 PK 플래그}). 앞쪽 #1 은
    실행 통계라 #1 을 모두 훑어야 한다.

    #8 은 기본키 컬럼에만 붙는다 (복합 기본키면 해당 컬럼마다 붙는 것을 확인했다).
    getDataTable 응답에는 이 정보가 없어 컬럼 메타는 항상 여기서 얻는다.
    """
    cols = []
    for d in frames:
        body = qp.extract_field_raw(d, 2)
        if not body:
            continue
        for fn, entry in qp.iter_fields(body):
            if fn != 1:
                continue
            meta = qp.extract_field_raw(entry, 3)
            if not meta:
                continue
            for fn2, val in qp.iter_fields(meta):
                if fn2 != 5:
                    continue
                items = qp.decode_raw(val)
                nm = qp.find_field(items, 2, "str")
                cols.append((nm if nm is not None else f"col{len(cols) + 1}",
                             qp.find_field(items, 3, "str") or "",
                             bool(qp.find_field(items, 8, "varint"))))
            if cols:
                return cols  # 첫 결과셋의 컬럼만 쓴다
    return cols


def extract_table(sql):
    """단순 SELECT 의 대상 (schema, table). 조인이나 다중 테이블이면 (None, None).

    클라이언트가 결과 행을 편집하려면 컬럼 정의의 테이블명이 필요하다. 확실한
    경우에만 채우고, 애매하면 비워서 잘못된 테이블로 편집이 일어나지 않게 한다.
    """
    if re.search(r"\bjoin\b", sql, re.I):
        return None, None
    m = re.search(r"\bfrom\s+`?([A-Za-z0-9_$]+)`?(?:\s*\.\s*`?([A-Za-z0-9_$]+)`?)?", sql, re.I)
    if not m or sql[m.end():].lstrip().startswith(","):
        return None, None
    return (m.group(1), m.group(2)) if m.group(2) else (None, m.group(1))


def parse_data_table(frames):
    """getDataTable 응답 → (컬럼명 리스트, 행 리스트). 값은 문자열이거나 None(NULL)."""
    cols, rows_blob = [], None
    for d in frames:
        body = qp.extract_field_raw(d, 2)
        if not body:
            continue
        for fn, val in qp.iter_fields(body):
            if fn == 4:
                nm = qp.find_field(qp.decode_raw(val), 2, "str")
                if nm is not None:
                    cols.append(nm)
            elif fn == 5:
                rows_blob = val
    rows = []
    if rows_blob is not None:
        decoded, _ = qp.mp_read(rows_blob, 0)
        for row in (decoded if isinstance(decoded, list) else []):
            cells = row.get("v", []) if isinstance(row, dict) else []
            out = []
            for cell in cells:
                if isinstance(cell, dict):
                    out.append(None if cell.get("n") else str(cell.get("v", "")))
                else:
                    out.append(str(cell))
            out += [None] * (len(cols) - len(out))
            rows.append(out[:len(cols)])
    return cols, rows


# --------------------------------------------------------------------------
# MySQL 프로토콜 패킷
# --------------------------------------------------------------------------
def lenenc_int(n):
    if n < 0xFB:
        return bytes([n])
    if n <= 0xFFFF:
        return b"\xfc" + n.to_bytes(2, "little")
    if n <= 0xFFFFFF:
        return b"\xfd" + n.to_bytes(3, "little")
    return b"\xfe" + n.to_bytes(8, "little")


def lenenc_str(b):
    if isinstance(b, str):
        b = b.encode("utf-8")
    return lenenc_int(len(b)) + b


def ok_packet(affected=0, insert_id=0):
    return (b"\x00" + lenenc_int(affected) + lenenc_int(insert_id)
            + STATUS_AUTOCOMMIT.to_bytes(2, "little") + b"\x00\x00")


def eof_packet():
    return b"\xfe" + b"\x00\x00" + STATUS_AUTOCOMMIT.to_bytes(2, "little")


def err_packet(msg, code=ERR_UNKNOWN, state="HY000"):
    return (b"\xff" + code.to_bytes(2, "little") + b"#" + state.encode()
            + msg.encode("utf-8", "replace")[:900])


def column_def(col, schema="", table=""):
    """컬럼 정의(protocol 41). col = (이름, CLR 타입, PK 여부).

    PK 컬럼에는 PRI_KEY 와 NOT_NULL 플래그를 세워야 클라이언트가 기본키로 인식하고
    행 복제, 인라인 편집을 정상 처리한다.
    """
    name, clr, is_pk = col
    mysql_type, flags = CLR_TO_MYSQL.get(clr, (TYPE_VAR_STRING, 0))
    if is_pk:
        flags |= FLAG_PRI_KEY | FLAG_NOT_NULL
    charset = CHARSET_BINARY if flags & (FLAG_NUM | FLAG_BINARY) else CHARSET_UTF8MB4
    decimals = 0x1F if mysql_type in (0x04, 0x05) else 0  # FLOAT/DOUBLE 은 자릿수 미정
    return (lenenc_str("def") + lenenc_str(schema) + lenenc_str(table) + lenenc_str(table)
            + lenenc_str(name) + lenenc_str(name)
            + lenenc_int(0x0C) + charset.to_bytes(2, "little")
            + (65535).to_bytes(4, "little") + bytes([mysql_type])
            + flags.to_bytes(2, "little") + bytes([decimals]) + b"\x00\x00")


def row_packet(values):
    out = b""
    for v in values:
        out += b"\xfb" if v is None else lenenc_str(v)
    return out


# --------------------------------------------------------------------------
# 클라이언트 커넥션 처리
# --------------------------------------------------------------------------
class Handler(socketserver.BaseRequestHandler):
    def setup(self):
        self.seq = 0
        self.db = None  # 이 클라이언트가 보고 있는 database

    # -- 패킷 입출력 ------------------------------------------------------
    def send(self, payload):
        self.request.sendall(len(payload).to_bytes(3, "little") + bytes([self.seq]) + payload)
        self.seq = (self.seq + 1) & 0xFF

    def recv_packet(self):
        head = self.recv_exact(4)
        if not head:
            return None
        length = int.from_bytes(head[:3], "little")
        self.seq = (head[3] + 1) & 0xFF
        return self.recv_exact(length)

    def recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.request.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    # -- 접속 절차 --------------------------------------------------------
    def handshake(self):
        salt = secrets.token_bytes(20)
        payload = (b"\x0a" + b"8.0.0-querypie-proxy\x00"
                   + secrets.randbelow(0xFFFF).to_bytes(4, "little")
                   + salt[:8] + b"\x00"
                   + (SERVER_CAPS & 0xFFFF).to_bytes(2, "little")
                   + bytes([CHARSET_UTF8MB4])
                   + STATUS_AUTOCOMMIT.to_bytes(2, "little")
                   + (SERVER_CAPS >> 16).to_bytes(2, "little")
                   + bytes([21]) + b"\x00" * 10
                   + salt[8:] + b"\x00" + b"mysql_native_password\x00")
        self.send(payload)

        resp = self.recv_packet()
        if not resp:
            return False
        caps = int.from_bytes(resp[:4], "little")
        i = 32
        end = resp.find(b"\x00", i)
        user = resp[i:end].decode("utf-8", "replace")
        i = end + 1
        # auth response 는 검사하지 않고 길이만큼 건너뛴다
        if caps & CAP_SECURE_CONNECTION:
            alen = resp[i]
            i += 1 + alen
        else:
            i = resp.find(b"\x00", i) + 1
        if caps & CAP_CONNECT_WITH_DB and i < len(resp):
            end = resp.find(b"\x00", i)
            db = resp[i:end].decode("utf-8", "replace")
            if db:
                self.db = db
        if self.server.login_from_client:
            if not self.authenticate_with_querypie(user):
                return False
        self.send(ok_packet())
        log(f"[proxy] 접속: user={user or '(없음)'} db={self.db or '(없음)'}")
        return True

    def authenticate_with_querypie(self, user):
        """클라이언트가 입력한 계정으로 QueryPie 에 로그인하고 credential 을 갱신한다.

        MySQL 기본 인증(mysql_native_password)은 비밀번호를 SHA1 챌린지로만 보내
        평문을 복원할 수 없다. 그래서 AuthSwitchRequest 로 mysql_clear_password 로
        바꿔 평문을 받는다 (프록시는 127.0.0.1 에만 바인딩한다).
        클라이언트가 이 전환을 거부하면 접속이 끊기므로, 그때는 이 옵션을 끄면 된다.

        비밀번호는 로그에 남기지 않는다.
        """
        session = self.server.session
        self.send(bytes([0xFE]) + b"mysql_clear_password" + bytes([0]))
        packet = self.recv_packet()
        if not packet:
            log("[proxy] 클라이언트가 평문 비밀번호 전환을 거부했습니다 "
                "(--login-from-client 를 끄고 쓰세요)")
            return False
        password = packet.rstrip(bytes([0])).decode("utf-8", "replace")
        if not user or not password:
            self.send(err_packet("사용자명과 비밀번호를 입력하세요.", 1045, "28000"))
            return False
        if not qp.login(session.insecure, session.window_id, cred=(user, password)):
            self.send(err_packet(f"QueryPie 인증에 실패했습니다 (user={user}). "
                                 "계정과 비밀번호를 확인하세요.", 1045, "28000"))
            return False
        # 이후 토큰 갱신, 재로그인이 이 계정을 쓰도록 메모리에 둔다.
        # 파일에는 쓰지 않는다 (querypie-login.json 은 CLI 전용).
        qp.set_credential(user, password, use_file=False)
        log(f"[proxy] {user} 로 QueryPie 인증 성공 (credential 은 메모리에만 둡니다)")
        return self.server.session.ensure_ready()

    # -- 응답 헬퍼 --------------------------------------------------------
    def send_error(self, msg):
        log(f"[proxy] 오류 응답: {msg[:160]}")
        self.send(err_packet(msg))

    def send_resultset(self, cols, rows, sql=""):
        schema, table = extract_table(sql)
        self.send(lenenc_int(len(cols)))
        for c in cols:
            self.send(column_def(c, schema or self.db or "", table or ""))
        self.send(eof_packet())
        for r in rows:
            self.send(row_packet(r))
        self.send(eof_packet())

    # -- 커맨드 루프 ------------------------------------------------------
    def handle(self):
        # 클라이언트가 창을 닫거나 연결을 끊는 것은 정상 종료다. 소켓 예외를 그대로
        # 두면 스레드마다 스택 트레이스가 찍혀 로그가 읽기 어려워진다.
        try:
            self.serve()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError):
            pass

    def serve(self):
        session = self.server.session
        if not self.handshake():
            return
        while True:
            body = self.recv_packet()
            if not body:
                return
            cmd, arg = body[0], body[1:]
            if cmd == COM_QUIT:
                return
            if cmd == COM_PING:
                self.send(ok_packet())
                continue
            if cmd == COM_INIT_DB:
                self.db = arg.decode("utf-8", "replace")
                self.send(ok_packet())
                continue
            if cmd == COM_FIELD_LIST:
                self.send(eof_packet())  # deprecated. 빈 목록으로 응답한다
                continue
            if cmd == COM_STATISTICS:
                # 서버 상태 문자열을 그대로 담는 단일 패킷. 클라이언트가 접속 상태를
                # 확인하는 용도라 형식만 맞으면 된다.
                self.send(b"Uptime: 0  Threads: 1  Questions: 0  Open tables: 0  "
                          b"Queries per second avg: 0.000")
                continue
            if cmd != COM_QUERY:
                name = COMMAND_NAMES.get(cmd, f"0x{cmd:02x}")
                self.send_error(f"지원하지 않는 명령 {name} 입니다 (텍스트 쿼리만 중계합니다)")
                continue

            sql = arg.decode("utf-8", "replace").strip().rstrip(";").strip()
            if self.server.trace:
                log(f"[proxy] << {sql[:400]}")
            self.run_query(session, sql)

    def run_query(self, session, sql):
        if not sql:
            self.send(ok_packet())
            return

        m = RE_USE.match(sql)
        if m:
            self.db = m.group(1)
            try:
                with session.lock:
                    session.change_db(self.db)
            except QpError as e:
                self.send_error(str(e))
                return
            log(f"[proxy] USE {self.db}")
            self.send(ok_packet())
            return

        if RE_SWALLOW.match(sql):
            # QueryPie 는 결과셋 없는 문장을 거부한다. 접속 절차가 막히지 않게 OK 로 답한다.
            self.send(ok_packet())
            return

        if not self.server.allow_write and not RE_READONLY.match(sql):
            self.send_error("읽기 전용 프록시입니다. 쓰기 문장은 --allow-write 로 실행하세요.")
            return

        if not RE_READONLY.match(sql):
            session.clear_cache()  # 스키마를 바꿨을 수 있으므로 캐시를 신뢰하지 않는다

        t = time.time()
        try:
            cols, rows = session.query(sql, self.db)
        except QpError as e:
            self.send_error(str(e))
            return
        except Exception as e:  # 프로토콜 오류로 프록시가 죽지 않도록 방어
            self.send_error(f"프록시 내부 오류: {e}")
            return
        pk = ",".join(c[0] for c in cols if c[2])
        # SQL 을 실제로 실행하지 않고 답한 경우 어느 경로였는지 표시한다.
        #   dict  = dictionary API        empty = 없음을 확인하고 빈 결과
        #   ddl   = CREATE TABLE 파싱     cache = 설정값 캐시   map = 미리 받아 둔 값
        elapsed = time.time() - t
        source = session.last_source
        tag = "" if source == "sql" else paint(f"[{source}] ", SOURCE_COLOR.get(source, ""))
        # 오래 걸린 것은 시간에도 색을 준다 (실제 서버 왕복이 있었다는 뜻)
        took = paint(f"{elapsed:.2f}s", C_YELLOW if elapsed >= 0.3
                     else (C_GRAY if elapsed < 0.1 else ""))
        log(f"[proxy] {took}  {tag}{sql[:200]}"
            f"  -> {len(cols)}컬럼 {len(rows)}행" + (f" PK={pk}" if pk else ""))
        if not cols:
            self.send(ok_packet())
            return
        self.send_resultset(cols, rows, sql)


class Server(socketserver.ThreadingTCPServer):
    # Windows 의 SO_REUSEADDR 는 리눅스와 달라 같은 포트를 다른 프로세스가 이미 듣고
    # 있어도 바인딩이 성공한다. 그러면 낡은 프록시가 계속 연결을 받아, 새로 띄운
    # 쪽이 도는 줄 알고 옛 동작을 계속 보게 된다. 포트가 물려 있으면 바로 실패시킨다.
    allow_reuse_address = False
    daemon_threads = True
    # main() 이 덮어쓴다. 여기 기본값이 있어야 이 서버를 직접 만들어 쓰는 경우에도 뜬다.
    trace = False
    allow_write = False
    login_from_client = False


def keep_alive(session, interval=600):
    """access token 은 발급 후 20분 남짓이라 주기적으로 갱신해 접속을 유지한다."""
    while True:
        time.sleep(interval)
        try:
            if qp._refresh_tokens(session.insecure, session.window_id):
                log("[proxy] 토큰 갱신")
            elif qp.login(session.insecure, session.window_id):
                log("[proxy] 재로그인")
        except SystemExit as e:
            log(f"[proxy] 토큰 갱신 실패: {humanize(str(e))}")


def main():
    ap = argparse.ArgumentParser(description="QueryPie 백엔드 로컬 MySQL 프록시")
    ap.add_argument("--conn-name",
                    help="커넥션 이름. 클러스터 이름의 앞부분이면 된다 (`shop-cluster` 면 `shop`). "
                         "미지정 시 querypie-config.json 의 default_connection 을 쓴다. "
                         "후보는 querypie_conn.py list 로 확인한다")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--host", default="127.0.0.1", help="바인딩 주소 (인증을 하지 않으므로 로컬 고정 권장)")
    ap.add_argument("--max-rows", type=int, default=1000, help="쿼리당 가져올 최대 행 수")
    ap.add_argument("--lob-max-bytes", type=int, default=1 << 20,
                    help="LOB 한 개당 받아올 최대 바이트 (기본 1MB)")
    ap.add_argument("--lob-cells", type=int, default=200,
                    help="쿼리당 전체 값으로 복원할 LOB 셀 수 상한 (0 이면 복원하지 않고 "
                         "미리보기 그대로 둔다). LOB 하나마다 왕복이 한 번 더 든다")
    ap.add_argument("--allow-write", action="store_true", help="SELECT/SHOW 외 문장도 QueryPie 로 전달")
    ap.add_argument("--insecure", action="store_true", help="TLS 검증 생략")
    ap.add_argument("--login", action="store_true", help="시작 시 credential 로 강제 재로그인")
    ap.add_argument("--experimental-ddl", action="store_true",
                    help="[실험] SHOW KEYS 를 CREATE TABLE 파싱(sqlglot)으로 처리한다. "
                         "약 10배 빠르지만 Cardinality 는 NULL 이 된다")
    ap.add_argument("--no-warmup", action="store_true",
                    help="시작 시 컬럼 헤더 예열을 하지 않는다 (첫 조회가 느려지는 대신 "
                         "기동 직후 요청이 전혀 나가지 않는다)")
    ap.add_argument("--no-dictionary", action="store_true",
                    help="개체 탐색을 dictionary API 로 대체하지 않고 전부 SQL 로 처리한다")
    ap.add_argument("--login-from-client", action="store_true",
                    help="클라이언트가 접속할 때 입력한 계정/비밀번호로 QueryPie 에 "
                         "로그인한다. 그 계정은 메모리에만 두고 querypie-login.json 은 "
                         "읽지도 쓰지도 않는다 (그 파일은 CLI 전용). 평문을 받으려고 "
                         "인증 방식을 바꾸므로 거부하는 클라이언트도 있다")
    ap.add_argument("--no-color", action="store_true",
                    help="로그에 색을 쓰지 않는다 (터미널이 아니면 자동으로 꺼진다)")
    ap.add_argument("--log-file", help="로그를 이 파일에도 남긴다 (클라이언트 동작 진단용)")
    ap.add_argument("--trace", action="store_true",
                    help="받은 SQL 을 실행 전에 그대로 로그에 남긴다 (오류로 끝난 쿼리도 보인다)")
    args = ap.parse_args()

    global _LOG_FP, _COLOR
    _COLOR = not args.no_color and enable_ansi()
    if args.log_file:
        _LOG_FP = open(args.log_file, "a", encoding="utf-8")

    args.conn_name = args.conn_name or qp.config().get("default_connection")
    if not args.conn_name:
        ap.error("--conn-name 이 필요합니다 "
                 "(querypie-config.json 의 default_connection 으로 기본값을 둘 수 있습니다)")

    session = QuerySession(args.conn_name, args.max_rows, args.insecure,
                           args.lob_max_bytes, args.lob_cells, not args.no_dictionary,
                           args.experimental_ddl, prefer_writer=args.allow_write)
    if args.login_from_client:
        qp.set_credential(use_file=False)  # 이 프로세스는 LOGIN_FILE 을 보지 않는다
    if args.login:
        qp.login(args.insecure, session.window_id)
    qp._refresh_tokens(args.insecure, session.window_id) or qp.login(args.insecure, session.window_id)

    warm = not args.no_dictionary and not args.no_warmup
    try:
        session.open()
    except (QpError, SystemExit) as e:
        if not args.login_from_client:
            raise
        # 아직 계정을 모른다 (쿠키도 만료). 첫 클라이언트 접속 뒤로 미룬다.
        session.warm_on_ready = warm
        log(f"[proxy] 세션을 아직 열지 못했습니다 ({e}). 클라이언트 접속을 기다립니다.")

    threading.Thread(target=keep_alive, args=(session,), daemon=True).start()
    if warm and session.ready:
        # 클라이언트를 기다리게 하지 않도록 백그라운드에서 예열한다.
        threading.Thread(target=session.warm_headers, daemon=True).start()

    try:
        server = Server((args.host, args.port), Handler)
    except OSError as e:
        sys.exit(f"[proxy] {args.host}:{args.port} 를 열 수 없습니다: {e}\n"
                 "  이미 다른 프록시가 그 포트를 쓰고 있을 수 있습니다. 그 창을 닫거나 "
                 "--port 로 다른 포트를 쓰세요.")
    server.session = session
    server.allow_write = args.allow_write
    server.trace = args.trace
    server.login_from_client = args.login_from_client
    log(f"[proxy] 버전 {VERSION}")
    log(f"[proxy] {args.host}:{args.port} 대기 중 (커넥션 {args.conn_name}, "
        f"{'쓰기 허용' if args.allow_write else '읽기 전용'}, 최대 {args.max_rows}행)")
    if args.login_from_client:
        log(f"[proxy] HeidiSQL: 호스트 {args.host}, 포트 {args.port}, "
            "사용자/암호는 QueryPie 계정 (이 값으로 QueryPie 에 로그인한다)")
    else:
        log(f"[proxy] HeidiSQL: 호스트 {args.host}, 포트 {args.port}, "
            "사용자/암호는 아무 값 (검사하지 않는다. QueryPie 계정은 querypie-login.json 을 쓴다)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("[proxy] 종료")


if __name__ == "__main__":
    main()
